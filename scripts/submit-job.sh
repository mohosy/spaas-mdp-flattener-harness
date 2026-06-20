#!/usr/bin/env bash
# Submit the flattener job to the running Flink session cluster.
#   JOB_CLASS=...  TARGET_TABLE=...  PROCESSOR_VERSION=...  scripts/submit-job.sh
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose --env-file docker/.env -f docker/docker-compose.yml"

JAR="${JAR:-/jobs/spaas-processor.jar}"
# Default = the config-driven flattener (M2+). RawEventsJob (M1) still works:
#   JOB_CLASS=com.example.spaas.job.RawEventsJob TARGET_TABLE=mdp.raw_events scripts/submit-job.sh
JOB_CLASS="${JOB_CLASS:-com.example.spaas.job.FlattenerJob}"
CONFIG_PATH="${CONFIG_PATH:-/config/job.yaml}"
TARGET_TABLE="${TARGET_TABLE:-mdp.raw_events}"
PROCESSOR_VERSION="${PROCESSOR_VERSION:-0.1.0-m1}"
KAFKA_TOPIC="${KAFKA_TOPIC:-mdp.mfg.raw}"

echo "== submitting ${JOB_CLASS} (config=${CONFIG_PATH}) =="
$COMPOSE exec -T \
  -e CONFIG_PATH="$CONFIG_PATH" \
  -e KAFKA_BOOTSTRAP=kafka:9092 \
  -e KAFKA_TOPIC="$KAFKA_TOPIC" \
  -e ICEBERG_REST_URI=http://iceberg-rest:8181 \
  -e ICEBERG_WAREHOUSE=s3://warehouse/ \
  -e S3_ENDPOINT=http://minio:9000 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  -e AWS_REGION=us-east-1 \
  -e TARGET_TABLE="$TARGET_TABLE" \
  -e PROCESSOR_VERSION="$PROCESSOR_VERSION" \
  flink-jobmanager flink run -d -c "$JOB_CLASS" "$JAR"

echo "== running jobs =="
$COMPOSE exec -T flink-jobmanager flink list
