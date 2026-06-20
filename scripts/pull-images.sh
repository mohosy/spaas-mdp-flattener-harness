#!/usr/bin/env bash
# Resilient image puller. The Docker registry CDN occasionally throws TLS
# handshake timeouts on big blobs; pulling one image at a time with retries
# (each resumes from cached layers) is far more reliable than one bulk pull.
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . docker/.env; set +a

IMAGES=(
  "$MINIO_IMAGE"
  "$MC_IMAGE"
  "$KAFKA_IMAGE"
  "$ICEBERG_REST_IMAGE"
  "$FLINK_IMAGE"
  "$TRINO_IMAGE"
)
RETRIES="${PULL_RETRIES:-12}"

for img in "${IMAGES[@]}"; do
  if docker image inspect "$img" >/dev/null 2>&1; then
    echo "== already present: $img"; continue
  fi
  ok=0
  for attempt in $(seq 1 "$RETRIES"); do
    echo "== pulling ($attempt/$RETRIES): $img"
    if docker pull "$img"; then ok=1; break; fi
    echo "   retry in 5s..."; sleep 5
  done
  if [ "$ok" -ne 1 ]; then
    echo "!! FAILED to pull $img after $RETRIES attempts"; exit 1
  fi
done
echo "== all images present =="
docker images --format '{{.Repository}}:{{.Tag}}  {{.Size}}' | grep -Ei 'kafka|minio|iceberg|flink|trino'
