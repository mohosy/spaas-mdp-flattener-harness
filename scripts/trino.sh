#!/usr/bin/env bash
# Run SQL against Trino using the CLI bundled in the trino container.
#   scripts/trino.sh -e "SELECT 1"          # inline statement
#   scripts/trino.sh -f path/to/file.sql    # statements from a file
set -euo pipefail
COMPOSE="docker compose --env-file docker/.env -f docker/docker-compose.yml"

mode="${1:- -e}"
case "$mode" in
  -e)
    sql="${2:?usage: trino.sh -e \"SQL\"}"
    $COMPOSE exec -T trino trino --server localhost:8080 --catalog iceberg \
      --output-format TSV --execute "$sql"
    ;;
  -f)
    file="${2:?usage: trino.sh -f file.sql}"
    $COMPOSE exec -T trino trino --server localhost:8080 --catalog iceberg \
      --output-format TSV < "$file"
    ;;
  *)
    echo "usage: trino.sh [-e \"SQL\" | -f file.sql]" >&2; exit 2
    ;;
esac
