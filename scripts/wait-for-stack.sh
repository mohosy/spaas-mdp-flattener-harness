#!/usr/bin/env bash
# Host-side readiness gate for the whole stack. The java/minio images ship no
# curl, so we probe from the host (which has curl + nc) instead of relying on
# in-container healthchecks for everything.
set -uo pipefail

TIMEOUT="${WAIT_TIMEOUT:-180}"
deadline=$(( $(date +%s) + TIMEOUT ))

note() { printf '  %-16s %s\n' "$1" "$2"; }

wait_for() {
  local name="$1"; shift
  printf '%-18s' "waiting $name"
  while true; do
    if "$@" >/dev/null 2>&1; then echo " OK"; return 0; fi
    if [ "$(date +%s)" -ge "$deadline" ]; then
      echo " TIMEOUT after ${TIMEOUT}s"; return 1
    fi
    printf '.'; sleep 2
  done
}

check_kafka()   { nc -z localhost 29092; }
check_minio()   { curl -sf http://localhost:9000/minio/health/live; }
check_rest()    { curl -sf http://localhost:8181/v1/config; }
check_trino()   { curl -sf http://localhost:8080/v1/info | grep -q '"starting":false'; }
check_flink()   { curl -sf http://localhost:8081/overview; }

echo "== Waiting for SPaaS stack to become ready (timeout ${TIMEOUT}s) =="
rc=0
wait_for "kafka"        check_kafka || rc=1
wait_for "minio"        check_minio || rc=1
wait_for "iceberg-rest" check_rest  || rc=1
wait_for "trino"        check_trino || rc=1
wait_for "flink"        check_flink || rc=1

echo
if [ "$rc" -eq 0 ]; then
  echo "== Stack READY =="
  note "Kafka"        "localhost:29092 (host) / kafka:9092 (containers)"
  note "MinIO"        "http://localhost:9001 (console: admin/password)"
  note "Iceberg REST" "http://localhost:8181/v1/config"
  note "Trino"        "http://localhost:8080 (catalog: iceberg)"
  note "Flink UI"     "http://localhost:8081"
else
  echo "== Stack NOT fully ready — check 'make logs' =="
fi
exit "$rc"
