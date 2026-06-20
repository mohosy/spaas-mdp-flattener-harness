# SPaaS MDP Flattener Harness

A local, runnable proof-of-concept of a streaming data service that consumes
manufacturing test events from **Kafka**, flattens nested JSON into one flat row
per event with **Apache Flink**, writes the rows to **Apache Iceberg** on
**S3-compatible storage** (MinIO) via an **Iceberg REST catalog**, and makes them
queryable by an external SQL engine (**Trino**, standing in for Snowflake).

It mirrors a production pipeline (manufacturing fixture → RabbitMQ → Kafka → Flink
flattener → externally-owned Iceberg-on-S3 read by Snowflake through an AWS Glue
REST catalog) using local components that speak the **same wire protocols**, so the
same Iceberg tables can later be pointed at real Snowflake/Glue without changing the
processor. All data is synthetic.

> Status: M0–M6 built and verified end to end. See [`NOTES.md`](NOTES.md) for the
> per-milestone verification output and the confirmed version matrix.

---

## Architecture

```
   generator (Python)                      Flink job (Java 17, DataStream API)
   synthetic mdp.mfg.v1     ┌────────────────────────────────────────────────┐
   envelopes                │  KafkaSource ─► keyBy ─► MdpFlattener (process) │
        │                   │                              │  │  │             │
        ▼                   │            flattened ◄───────┘  │  └──► audit    │
  ┌───────────┐  mdp.mfg.raw│            (main out)    quarantine  (per window)│
  │   Kafka    ├────────────►│                │             │        │          │
  │  (KRaft)   │            └────────────────┼─────────────┼────────┼──────────┘
  └───────────┘                              ▼             ▼        ▼
                                    ┌──────────────────────────────────────┐
                                    │  Iceberg REST catalog  ◄── metadata   │
                                    │  (apache/iceberg-rest-fixture)        │
                                    └──────────────────┬───────────────────┘
                                                       │ data + metadata (S3FileIO)
                                                       ▼
                                            ┌────────────────────┐
                                            │  MinIO (S3 API)     │
                                            │  s3://warehouse/    │
                                            └─────────┬──────────┘
                                                      │ reads same tables, same catalog
                       ┌──────────────┐               ▼
                       │  demo API    │◄──── Trino ──► mdp.flattened_measurements
                       │  (FastAPI)   │     (external  mdp.quarantine
                       └──────────────┘      SQL engine) mdp.audit  / mdp.raw_events
```

| Production | Local stand-in | Why faithful |
|---|---|---|
| Kafka | Apache Kafka (KRaft) | same wire protocol + consumer semantics |
| Flink flattener | Apache Flink 1.20, Java 17, DataStream API | same engine/API |
| AWS S3 | MinIO | same S3 API |
| AWS Glue REST catalog | apache/iceberg-rest-fixture | same Iceberg REST protocol |
| Iceberg tables | Iceberg on MinIO via REST | identical table format/metadata |
| Snowflake (external reader) | Trino via the REST catalog | external SQL engine on the same tables |

---

## Prerequisites

- **Docker** + **Docker Compose v2** (Docker Desktop). The stack wants ~6 GB of
  images and runs ~6 JVM services — give Docker **≥ 8 GB RAM** (heaps are capped to fit).
- **Java 17** (only needed if you build outside Docker; by default the jar is built
  in a `gradle:8.10-jdk17` container, so no host Gradle/JDK is required).
- **Python 3.9+** (host venv for the generator + demo API).
- ~6 GB free disk for images.

> If `docker pull` fails with TLS errors (e.g. behind a captive portal), use
> `bash scripts/pull-images.sh` which retries each image individually.

---

## Quick start (one command)

```bash
make demo-full
```

Brings the stack up, creates the Iceberg tables, builds + submits the Flink job,
produces a burst (with malformed + duplicate injection), and starts the demo API.
Then open:

- Demo API — http://localhost:8000 (OpenAPI docs at `/docs`)
- Flink UI — http://localhost:8081
- MinIO console — http://localhost:9001 (`admin` / `password`)
- Trino — http://localhost:8080

## Step by step

```bash
make up                 # bring up Kafka, MinIO, Iceberg REST, Flink, Trino; wait until ready
make create-tables      # create the mdp.* Iceberg tables via Trino
make build-job          # build the Flink fat jar (Gradle in Docker)
make venv               # create the Python venv (generator + demo deps)
make submit-job         # submit the config-driven FlattenerJob
make produce N=500 MALFORMED=5 DUP=10   # produce synthetic events (inject bad + dup records)
make freshness N=300    # measure Kafka->queryable latency (P50/P95/P99)
make demo               # start the FastAPI demo on :8000
make ps | make logs | make down         # operate the stack
```

Run `make help` for the full target list.

### Querying

```bash
make trino-sql SQL="SELECT count(*) FROM iceberg.mdp.flattened_measurements"
make trino-sql SQL="SELECT error_reason, count(*) FROM iceberg.mdp.quarantine GROUP BY 1"
make trino-sql SQL="SELECT * FROM iceberg.mdp.audit ORDER BY commit_timestamp DESC LIMIT 5"
```

---

## Data contract (synthetic placeholder)

A Kafka message is an **envelope** that carries one or many **events**; the flattener
emits **one row per event**. The placeholder shape is `mdp.mfg.v1` (see
[`config/field-mapping.yaml`](config/field-mapping.yaml)). Required fields for a valid
event: `eventId`, `productSerial`, `measurementName`, `measurementValue`, `measuredAt`.
A message that can't be parsed, or an event missing a required field, goes to
**quarantine** — the pipeline never crashes on a bad record.

Each output row has 16 business columns + lineage columns (`source_topic/partition/
offset`, `kafka_timestamp`, `raw_payload_hash`, `processor_version`,
`canonical_row_hash`, `processed_at`). `canonical_row_hash` (sha256 of a deterministic,
key-sorted, null-safe serialization of the business columns) defines row identity and
is the key for **intra-message deduplication**.

---

## How to swap in the real MDP topic and schema

Everything manufacturing-specific is isolated. To point this at the real systems,
edit **config**, not code:

1. **Real event schema** — edit [`config/field-mapping.yaml`](config/field-mapping.yaml)
   (the file is marked `REPLACE WITH REAL SCHEMA`). Map the real envelope keys, the
   per-column event field names, and the `required_fields` list to the real
   `testMeasurementEvent`. If the real business columns differ, also update the column
   list/types in
   [`FlattenedSchema.java`](processor/src/main/java/com/example/spaas/api/FlattenedSchema.java)
   and the matching DDL in [`scripts/sql/create_tables.sql`](scripts/sql/create_tables.sql).
   The flattener (`MdpFlattener`) and job wiring do **not** change.
2. **Real Kafka topic / brokers** — edit `source.*` in
   [`config/job.yaml`](config/job.yaml) (`bootstrap_servers`, `topic`, `group_id`,
   `startup_mode`). For a secured cluster, add the relevant `KafkaSource` properties
   (SASL/SSL) in `FlattenerJob.buildPipeline` via `KafkaSource.builder().setProperties(...)`.
   Keep using the **vanilla** `flink-connector-kafka` (no internal wrapper).
3. **Real Snowflake / Glue catalog** — the processor only knows an Iceberg REST
   endpoint. Point `iceberg.rest_uri` / `warehouse` / `s3_endpoint` in
   [`config/job.yaml`](config/job.yaml) at the real Glue REST catalog + S3, and supply
   real credentials via env/Vault (never the committed defaults). To **read** the
   tables with real Snowflake instead of Trino, register the same Iceberg tables as a
   Snowflake external/Iceberg table via the same catalog — no processor change. (The
   `docker/trino` config is only the local query stand-in.)
4. **Processor identity** — set `processor.version` in `job.yaml` (flows to the
   `processor_version` lineage column).

Search the repo for `REPLACE WITH REAL SCHEMA` and `TODO: confirm with mentors` for
every spot that needs a real value.

---

## Tests

```bash
make test               # fast JUnit unit tests (flattener logic, hashing, dedup, quarantine)
make test integration   # + Testcontainers end-to-end (ephemeral Kafka+MinIO+REST, real pipeline)
```

The integration test runs the exact `FlattenerJob.buildPipeline` on a Flink MiniCluster
against throwaway containers and asserts good rows land, bad rows quarantine, and audit
is written.

---

## Delivery semantics

Flink checkpoints to MinIO (`s3p://warehouse/checkpoints`). Kafka offsets are stored in
checkpoints and the Iceberg `FlinkSink` committer is idempotent per checkpoint, giving
**end-to-end exactly-once** for committed rows. Verified by killing the TaskManager
mid-stream during a 1000-event burst: the job resumed from checkpoint with exactly 1000
rows — no loss, no duplicates (see M4 in [`NOTES.md`](NOTES.md)).

## Project layout

```
docker/        docker-compose.yml + Trino/Flink/MinIO/observability config; .env pins all image versions
processor/     Java 17 Gradle module (repo-portable): api/ transform/mdp/ lineage/ job/ config/ + tests
               versions.gradle is the single source of dependency versions
generator/     Python synthetic event generator (rate/burst, malformed + duplicate injection)
demo-api/      FastAPI demo (queries through Trino)
scripts/       build/submit/freshness/demo helpers + create_tables.sql
config/         job.yaml (single job config) + field-mapping.yaml (REPLACE WITH REAL SCHEMA)
Makefile  README.md  NOTES.md
```

## Constraints honored

Open-source/local only (vanilla `flink-connector-kafka`); no real cloud/Snowflake/Vault;
synthetic data only; single writer per Iceberg table (parallelism 1, enforced in
`JobConfig`); intra-message dedup only; manufacturing schema isolated behind one
config + mapping file; the `processor/` module is self-contained and liftable into a
Java 17 Gradle multi-project repo.
