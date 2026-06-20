#!/usr/bin/env bash
# Cancel all running Flink jobs.
set -euo pipefail
cd "$(dirname "$0")/.."
COMPOSE="docker compose --env-file docker/.env -f docker/docker-compose.yml"
ids=$($COMPOSE exec -T flink-jobmanager flink list -r 2>/dev/null \
      | grep -oE '[0-9a-f]{32}' || true)
if [ -z "$ids" ]; then echo "no running jobs"; exit 0; fi
for id in $ids; do
  echo "cancelling $id"
  $COMPOSE exec -T flink-jobmanager flink cancel "$id" || true
done
