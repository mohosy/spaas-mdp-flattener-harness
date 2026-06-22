# NOTES — SPaaS MDP flattener harness

Running build log. One section per milestone with the exact verification output.

## Environment (2026-06-19)
- Docker 29.2.1, Compose v2 (5.0.2). Docker VM: **4 CPU, 8 GB RAM** (tight).
- Disk: ~16 GiB free, volume 99% full → images tuned/pinned; Python on host.
- Java: Temurin **17.0.16** (default `java`/`javac`). Gradle NOT installed →
  Flink fat jar is built with `gradle:8.x-jdk17` in Docker (`scripts/build-job.sh`).
- Python 3.9.6 on host (generator + demo run in `.venv`).

## Version matrix (confirmed against Maven Central + Docker Hub, not memory)
| Component | Pin | Notes |
|---|---|---|
| Flink | `flink:1.20.4-java17` | Chose 1.20 over 2.x: most battle-tested Iceberg support |
| flink-connector-kafka | `3.4.0-1.20` | Vanilla connector; version suffix MUST match Flink minor |
| Iceberg (lib/runtime/aws-bundle) | `1.10.1` | `iceberg-flink-runtime-1.20:1.10.1`, `iceberg-aws-bundle:1.10.1` |
| Iceberg REST catalog | `apache/iceberg-rest-fixture:1.10.1` | Pinned to SAME Iceberg version as Flink lib → no metadata skew |
| Kafka | `apache/kafka:3.9.2` | KRaft single node, no ZooKeeper |
| MinIO | `minio/minio:RELEASE.2025-09-07T16-13-09Z` + `minio/mc` | S3 API stand-in |
| Trino | `trinodb/trino:481` | External SQL engine; stand-in for Snowflake |

> **TODO: confirm with mentors** — the production Flink/Iceberg target version is
> still unconfirmed (prod repo has a mismatch: some modules on 1.19, some on 2.x).
> This harness deliberately standardizes on Flink **1.20.4** for reliability.
> Image tags live in `docker/.env`; Gradle dep versions in `processor/versions.gradle`.

### Compatibility resolutions / decisions
- **Kafka listeners**: internal `PLAINTEXT://kafka:9092` for containers, external
  `EXTERNAL://localhost:29092` for host clients (generator, Testcontainers).
- **MinIO has no curl/wget** (ubi-micro base) → readiness gated by the `minio-init`
  (`mc ready`) container + host-side `scripts/wait-for-stack.sh`, not a container healthcheck.
- **Iceberg REST persistence**: JDBC/sqlite catalog on a named volume so namespaces
  survive `make down` (warehouse *data* lives on MinIO; sqlite stores the pointers).
- **Flink checkpoints → MinIO** via the bundled `flink-s3-fs-presto` plugin
  (`s3p://` scheme), enabled from M0 so M4 needs no stack change.
- **Trino heap capped** to 1.5 GB (`docker/trino/jvm.config`) for the 8 GB VM.

---

## M0 — Infrastructure up  ✅ PASS (2026-06-19)

**Network note:** the host is behind an captive portal that intermittently
MITMs TLS to Docker's registry (`x509: certificate is valid for
<captive-portal-domain>`). Pulls needed per-image retries
(`scripts/pull-images.sh`); all 6 images eventually cached.

**Fix applied during M0:** the `apache/iceberg-rest-fixture` runs as uid 1000
(`iceberg`) but the persistence volume mounts root-owned → `SQLITE_CANTOPEN`. Added
`user: root` to the iceberg-rest service so the sqlite catalog DB is writable.

Verification output:
```
$ bash scripts/wait-for-stack.sh
== Stack READY ==
  Kafka  localhost:29092 | MinIO :9001 | Iceberg REST :8181 | Trino :8080 | Flink UI :8081

$ make smoke
-- create namespace mdp           CREATE SCHEMA
-- create empty table mdp.smoke   CREATE TABLE
-- count rows (expect 0)          row count = '0'
-- list tables in mdp             smoke
-- drop smoke table               DROP TABLE
== M0 SMOKE PASS: empty Iceberg table created + queried (0 rows) ==

$ curl .../8081/overview -> http_code=200   (Flink UI reachable)

$ docker compose ps
spaas-kafka          Up (healthy)
spaas-minio          Up
spaas-iceberg-rest   Up (healthy)
spaas-trino          Up (healthy)
spaas-flink-jobmanager / -taskmanager  Up
```
Proves Trino <-> Iceberg REST catalog <-> MinIO (S3) are wired end to end.

---

## M1 — Walking skeleton  ✅ PASS (2026-06-19)

Built the fat jar (`scripts/build-job.sh`, Gradle-in-Docker, zip64 enabled — the
Hadoop client + AWS bundle push the jar past 65535 entries). Submitted `RawEventsJob`
(Kafka -> Flink -> Iceberg `mdp.raw_events`). Produced 100 clean messages.

```
$ make produce N=100 RATE=0      -> produced=100
$ kafka-get-offsets mdp.mfg.raw  -> p0:35  p1:30  p2:35   (= 100)
$ submit-job (RawEventsJob)      -> JobID ... spaas-raw-events-m1 (RUNNING)
# poll raw_events:
  t=5s count=100   >>> reached 100   (visible ~5s after produce)

$ SELECT count(*), count(DISTINCT message_id), count(DISTINCT raw_payload_hash)
  100  |  100  |  100
$ sample row: message_id=uuid schema_version=mdp.mfg.v1 source_topic=mdp.mfg.raw
              source_partition=0 source_offset=0 raw_payload_hash=sha256 processor_version=0.1.0-m1
$ per-partition offsets: p0 n=35 [0..34]  p1 n=30 [0..29]  p2 n=35 [0..34]  (matches Kafka exactly)
```
Whole path Kafka -> Flink -> Iceberg -> Trino works with the real vanilla
`flink-connector-kafka`, Iceberg `FlinkSink`, REST catalog and S3FileIO on MinIO.

---

## M2 — Real flattening + correctness  ✅ PASS (2026-06-19)

`MdpFlattener` (behind the `Transform` interface, driven by `field-mapping.yaml`) +
config-driven `FlattenerJob`. One row per event, 16 business + 8 lineage columns,
`canonical_row_hash` over business columns, intra-message dedup by that hash.

**Unit tests (11/11 PASS)** — `scripts/test.sh` (Gradle-in-Docker):
```
HashingTest: sha256 stable/hex, order-independent hash, null-safe, value-sensitive   (4)
MdpFlattenerTest: happyPath/oneRowPerEvent, deterministicCanonicalRowHash,
  intraMessageDedup_dropsDuplicates, malformedJson_noCrash,
  missingRequiredField_validEventsStillFlow, missingEventsArray, invalidNumericValue (7)
BUILD SUCCESSFUL
```
(Fixed a Flink/Kafka lz4 capability conflict via `resolutionStrategy.capabilitiesResolution`.)

**End-to-end (Trino):** produced 200 msgs (15% dup-inject, 10% malformed) atop the 100 M1 msgs.
```
sample row: measurement_value=8.5946 < lower_limit 9.0 -> result FAIL  (logic intact)
            processor_version=0.2.0-mdp (from job.yaml), canonical_row_hash len=64
DEDUP CHECK (messages with a duplicate canonical_row_hash): 0
counts: 685 rows = 685 distinct hashes = 685 distinct event_ids   (exactly one row per unique event)
```

## M3 — Quarantine + audit  ✅ PASS (2026-06-19)

Flink side outputs -> `mdp.quarantine` (bad records) and `mdp.audit` (per-window run metadata).
```
quarantine by reason: MISSING_REQUIRED_FIELD=10  MALFORMED_JSON=9   (= 19 injected malformed)
  - detail e.g. "missing or null required field 'eventId'"; raw_payload captured
audit row: processor_version=0.2.0-mdp topic=mdp.mfg.raw
  input_event_count=727 output_row_count=685 quarantine_count=19 dedup_dropped_count=32
  snapshot_id=5146565617015845865  partition_offsets={"0":{min:0,max:109},"1":{...},"2":{...}}
```
**Audit math closes exactly: 727 input − 32 dedup − 10 quarantined-events = 685 output.**
Good events flow to flattened_measurements while bad ones quarantine; pipeline never crashes.

> Note: the generator's `dup_messages` summary undercounted (only counted dups on max-size
> messages); fixed to count every injected duplicate. Pipeline dedup itself was always correct
> (audit dedup_dropped=32 matches ~15% of 200, dedup-check query shows 0 survivors).

---

## M4 — Durability + replay  ✅ PASS (2026-06-19)

Flink checkpointing to MinIO every 10s via the bundled `flink-s3-fs-presto` plugin
(`s3p://warehouse/checkpoints`). Verified the checkpoint metadata on MinIO:
`481cee21.../chk-20/_metadata`.

**Delivery semantics: end-to-end EXACTLY-ONCE for committed rows.**
- Kafka source offsets are stored in Flink checkpoints (not relied upon from the Kafka
  consumer group), so recovery rewinds to the checkpointed offset.
- Iceberg `FlinkSink` uses two-phase commit: data files are committed only on checkpoint
  completion, and the committer tracks the max committed checkpoint id, so re-committing
  after a restart is idempotent (no double-commit).
- Flattening is deterministic, so replayed records reproduce identical rows/hashes.

**Replay test:** produced 1000 uniquely-tagged events (`test_session_id=replay-r1`), then
HARD-KILLED the taskmanager mid-stream (`docker compose kill flink-taskmanager`), then
restarted it.
```
after kill: taskmanager dies; job goes RESTARTING; on TM return -> RUNNING (resumed from chk)
poll: t=15s replay_rows=1000 (stable)
SELECT count(*), count(DISTINCT event_id), count(DISTINCT canonical_row_hash)
       WHERE test_session_id='replay-r1'
  -> 1000 | 1000 | 1000        (no lost events, no duplicate output rows)
duplicate event_id check: empty
taskmanager: Up 38 seconds     (confirms it was killed + restarted)
```
Exactly 1000 rows survived a mid-stream taskmanager crash: no loss, no duplicates beyond
what dedup handles.

---

## M5 — Freshness + demo  ✅ PASS (2026-06-19)

`scripts/freshness.py` (make freshness) produces a tagged burst, polls Trino until each
event is visible, and reports per-event latency (producedAt -> first queryable).
```
$ make freshness N=300
  visible: 300/300
  P50: 2.142s   P95: 2.144s   P99: 2.144s   min: 2.141  max: 2.145  mean: 2.143
```
Latency is dominated by the 10s Iceberg commit (checkpoint) interval — the burst landed
~2s before a commit, so all 300 became visible together. Lower `runtime.checkpoint_ms`
for fresher data (at the cost of more, smaller Iceberg snapshots).

FastAPI demo (make demo, http://localhost:8000) queries the tables THROUGH TRINO:
```
GET /health     -> {"status":"ok","trino":"localhost:8080"}
GET /latest     -> recent flattened rows (all 24 columns; e.g. measurement_value=298.0 PASS)
GET /freshness  -> last measured percentiles + live_lag_seconds (e.g. 34.8s)
GET /quarantine -> recent rejects (MALFORMED_JSON, ...)
GET /audit      -> recent run windows: {in:300,out:300,...}, {in:1000,out:1000,...}
```
Endpoints return live data while the generator/job run.

---

## M6 — Tests, metrics, docs  ✅ PASS (2026-06-19)

**Testcontainers integration test** (`make test integration`) spins ephemeral Kafka +
MinIO + Iceberg REST, runs the EXACT production pipeline (`FlattenerJob.buildPipeline`)
on a Flink MiniCluster, and asserts good/bad/audit outputs:
```
IcebergFilesCommitter - Committed append to table: rest.mdp.flattened_measurements ...
IcebergFilesCommitter - Committed append to table: rest.mdp.quarantine ...
IcebergFilesCommitter - Committed append to table: rest.mdp.audit ...
[IT] FINAL flattened=7 quarantine=2 audit=1
endToEnd_goodRowsLand_badRowsQuarantine_auditWritten() PASSED
12 tests completed (11 unit + 1 integration), 0 failed
```
The integration test caught two real environment gaps the fat jar relies on the Flink
*cluster* providing (and that the live stack has, but an in-JVM MiniCluster does not):
1. **`flink-connector-base`** — `KafkaSource`'s `RecordEmitter` (`ClassNotFoundException`);
   ships in the Flink dist `/lib`. Added as a `testImplementation`.
2. **JDK-17 `--add-opens`** — checkpointing failed with `InaccessibleObjectException`
   (`java.util` not opened); the Flink dist sets these via `env.java.opts`. Added the
   standard `--add-opens` set to the test JVM in `build.gradle`.

**Metrics (optional, `make up-obs`):** Flink's Prometheus reporter plugin is already in
the image (`/opt/flink/plugins/metrics-prometheus`) and is enabled via FLINK_PROPERTIES
(port 9249). Verified Prometheus scrapes it:
```
flink-jobmanager  http://flink-jobmanager:9249/metrics  -> up
flink-taskmanager http://flink-taskmanager:9249/metrics -> up
flink_jobmanager_numRunningJobs = 1
grafana: ok   (dashboard "SPaaS MDP Flattener — Flink" provisioned)
```

**Docs:** `README.md` with prerequisites, one-command quick start (`make demo-full`),
architecture diagram + production-mapping table, and a labeled
"How to swap in the real MDP topic and schema" section.

---

## M7 — Real MDP operations + durable, commit aligned audit  ✅ PASS (2026-06-22)

Two changes on top of the M2 flattener, both config driven and covered by tests.

**Four flatten operations (in `field-mapping.yaml`).** The mapping now supports four derived
column shapes taken from the production dbt model, alongside the existing direct mapping:
1. fallback chain (coalesce): the first source that is present and not null wins, else null.
2. array reduce: MAX or MIN over the numeric values of named entries (UL, LL) inside an array
   of {name, value} objects, comparing the name ignoring case and skipping values that are
   not numbers; a single match returns that value.
3. numeric and string split: one source string goes to a numeric column when it parses as a
   decimal, otherwise to a string column (a text value is kept, never quarantined).
4. multi format timestamp: the first accepted format wins (ISO 8601, then `MM/dd/yyyy`, then
   `yyyy-MM-dd`), else null; an unparseable value is nulled, not quarantined.
Operations are typed, self validating records (`MdpOperation`), so a misconfigured mapping
fails fast on load. `defaultSynthetic()` and `config/field-mapping.yaml` each carry one clear
example. The examples reference real MDP fields the synthetic generator does not emit, so the
live flattened table is unchanged; the operations are proven by unit tests on the output map.

**Durable, commit aligned audit window (`FlattenerJob`).** The audit counters (input, output,
quarantine, dedup, and per partition offset min and max) now live in Flink operator managed
state rather than in plain transient fields. processElement accumulates a running window;
snapshotState seals it into a pending window tagged with the checkpoint id and writes the
pending list to a ListState, so the counts survive a checkpoint and a restart;
notifyCheckpointComplete records the completed checkpoint id; a processing time timer emits
each pending window whose checkpoint has committed, stamped with the committed Iceberg
snapshot id. Emission runs from the timer (not from the next element), so a bounded run that
produces once and then waits still writes its audit row. The running window resets at each
checkpoint on purpose: a restart rewinds the Kafka source and reprocesses those records, so
persisting a partial window would double count. Flattened data stays exactly once; audit rows
are at least once.

Verification:
```
$ bash scripts/test.sh             -> 29 unit PASS (Hashing 4, MdpFieldMapping 6, MdpFlattener 19)
$ bash scripts/test.sh integration -> [IT] FINAL flattened=7 quarantine=2 audit=1   PASSED
  IcebergFilesCommitter committed flattened_measurements + quarantine at checkpointId 1,
  and audit at checkpointId 2: a window is sealed at its checkpoint, emitted after that
  checkpoint completes, and committed on the next one (audit lags one commit, by design).
```

---

## Summary

All milestones M0–M6 verified end to end on the live stack, plus a 12-test suite
(unit + Testcontainers). End-to-end exactly-once across a taskmanager crash. Freshness
~2.1s (bounded by the 10s commit interval). Everything is config-driven and the
manufacturing schema is isolated behind `field-mapping.yaml` (REPLACE WITH REAL SCHEMA).
