# RUNBOOK — how to run and test the SPaaS MDP flattener

An operational guide for running and testing this repo from a fresh clone, written so a
coding agent (or a person) can follow it step by step. The architecture is in
[`README.md`](README.md), the per milestone build log with verification output is in
[`NOTES.md`](NOTES.md), and a short project overview is in [`context.txt`](context.txt).

## What you need (fresh machine)

- Docker plus Docker Compose v2 (Docker Desktop is fine). Give Docker at least 8 GB of RAM:
  the full stack runs about six JVM services. The fast unit tests need only the Docker
  daemon, not the full stack.
- Python 3.9 or newer (only for the generator, the demo API, and the freshness probe).
- About 6 GB of free disk for images.
- You do NOT need host Gradle or a host JDK. The Flink jar and the tests build inside a
  `gradle:8.10-jdk17` container.

If `docker pull` fails with TLS errors (for example behind a captive portal), run
`bash scripts/pull-images.sh`, which retries each image one at a time.

## The fast gate: unit tests (do this first, every time)

```
bash scripts/test.sh
```

This runs the JUnit suite with Gradle inside Docker (no host Gradle needed). Expect
`BUILD SUCCESSFUL` and 29 passing tests:

- HashingTest (4): the canonical row hash is stable, order independent, null safe, and
  value sensitive.
- MdpFieldMappingTest (6): the four operations parse from YAML, and a misconfigured mapping
  fails fast.
- MdpFlattenerTest (19): the four operations (happy paths plus edge cases), determinism,
  intra message dedup, quarantine, and the original flatten behavior.

This is the primary correctness gate. It is pure JVM logic, so it is fast and needs no
running stack. Run it before and after any change.

Gradle caches results between runs. To force a full re-run:

```
docker run --rm -v "$PWD/processor":/work -v spaas-gradle-cache:/home/gradle/.gradle \
  -w /work gradle:8.10-jdk17 gradle --no-daemon test --rerun-tasks
```

## The integration test (slower, optional)

```
bash scripts/test.sh integration
```

This runs the exact production pipeline on a Flink MiniCluster against ephemeral Kafka,
MinIO, and an Iceberg REST catalog that Testcontainers starts (it talks to the host Docker
socket). Expect these lines near the end:

```
[IT] FINAL flattened=7 quarantine=2 audit=1
endToEnd_goodRowsLand_badRowsQuarantine_auditWritten() PASSED
```

It also confirms the audit row commits one checkpoint after the data it describes (data at
checkpointId 1, audit at checkpointId 2). It is slow because it starts real containers, so
treat the unit gate above as the fast loop and run this when you touch the job wiring or the
audit logic.

## Running the whole thing live (optional, heavy)

Use this to watch real data flow through and to measure freshness. It needs the full stack,
so about 8 GB of Docker RAM.

```
make up               # start Kafka, MinIO, Iceberg REST, Flink, Trino; wait until ready
make create-tables    # create the mdp.* Iceberg tables through Trino (idempotent)
make build-job        # build the Flink fat jar in Docker -> processor/build/libs/spaas-processor.jar
make venv             # create the Python venv (generator, demo, and freshness deps)
make submit-job       # submit the flattener to the Flink cluster (detached; returns immediately)
make freshness N=300  # produce 300 tagged events, poll Trino, report P50/P95/P99 latency
```

Then inspect the results through Trino:

```
make trino-sql SQL="SELECT count(*) FROM iceberg.mdp.flattened_measurements"
make trino-sql SQL="SELECT error_reason, count(*) FROM iceberg.mdp.quarantine GROUP BY 1"
make trino-sql SQL="SELECT * FROM iceberg.mdp.audit ORDER BY commit_timestamp DESC LIMIT 5"
```

When you are done:

```
make down             # stop the stack, keep the data volumes
make down-v           # stop the stack and delete the volumes (full reset)
```

Useful extras: `make ps` (container status), `make jobs` (running Flink jobs),
`make cancel-jobs` (cancel them), `make logs SVC=trino` (tail one service). The web UIs are
the Flink UI at http://localhost:8081, the MinIO console at http://localhost:9001
(admin / password), and Trino at http://localhost:8080.

### Things that will trip you up (read this)

- `make submit-job` uses `flink run -d`, so it is detached and returns right away. The
  streaming job keeps running on the cluster. Do not wait for it to finish, because it will
  not.
- The job reads `config/job.yaml` at submit time, and `startup_mode` is `earliest`, so a
  fresh submit replays the whole Kafka topic from the start. On a stack that already has
  data this adds duplicate synthetic rows (dedup is intra message only, not across runs).
  That is expected and harmless. For a clean count, reset first with `make down-v` then
  `make up`.
- If a job from a previous run is still active, cancel it before submitting a new one with
  `make cancel-jobs`.
- The freshness probe prints a line like
  `localhost:29092/bootstrap: ... ipv6 ... Connection refused`. That is benign; it falls
  back to IPv4 and still produces every event.
- Reading the freshness number honestly: a single `make freshness` burst lands in ONE
  Iceberg commit window, so P50, P95, and P99 come out nearly identical. That is one sample,
  not a distribution. Run it two or three times to watch the value move with commit timing.
  Freshness is bounded by the commit interval (5 seconds, set by `runtime.checkpoint_ms` in
  `config/job.yaml`), so the honest claim is P95 under 10 seconds, not the single lucky burst.

## How the recent work was verified (replicate this)

1. Unit gate: `bash scripts/test.sh` shows 29 passing.
2. Integration: `bash scripts/test.sh integration` shows flattened=7, quarantine=2, audit=1.
3. Live freshness (optional): bring the stack up, build and submit the job, run
   `make freshness N=300` two or three times, and confirm every burst is under 10 seconds.
   Also check the newest `mdp.audit` row: its counts should close (input minus dedup minus
   quarantined events equals output) and it should carry a snapshot_id that is not null.

## Where the important code lives

- `config/field-mapping.yaml` : the manufacturing schema mapping, including the four derived
  operations (fallback, array_reduce, numeric_string_split, multi_format_timestamp).
- `config/job.yaml` : the single job config (source, sinks, Iceberg target, checkpoint_ms).
- `processor/src/main/java/com/example/spaas/transform/mdp/` : MdpFlattener, MdpFieldMapping,
  and MdpOperation (the four operations).
- `processor/src/main/java/com/example/spaas/job/FlattenerJob.java` : the Flink pipeline and
  the audit window.
- `processor/src/test/java/com/example/spaas/...` : the unit and integration tests.

## Rules to keep (do not break these)

- Keep the two transform contracts: deterministic output (identical input gives identical
  rows and hashes) and never throwing on a bad record (bad input becomes a quarantine
  record). The unit tests enforce both.
- Do not change the version matrix (`processor/versions.gradle`, `docker/.env`), the docker
  compose stack, or the Kafka, Iceberg, or Trino wiring.
- Keep the Java package `com.example` (a privacy scrub for this public repo).
- Scope is the manufacturing flattener only. Do not add a generic plugin system or a
  transform registry.
- Work incrementally: change one piece, add or update its test, run `bash scripts/test.sh`
  to green, then move on.
