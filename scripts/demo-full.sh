#!/usr/bin/env bash
# One command to show the whole thing: up -> tables -> build -> submit -> produce -> API.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "############################################################"
echo "#  SPaaS MDP flattener harness — full demo"
echo "############################################################"

echo "== [1/7] bring up the stack =="
make up

echo "== [2/7] create Iceberg tables =="
make create-tables

echo "== [3/7] build the Flink fat jar (if missing) =="
[ -f processor/build/libs/spaas-processor.jar ] || make build-job

echo "== [4/7] python venv (if missing) =="
[ -d .venv ] || make venv

echo "== [5/7] (re)submit the flattener job =="
make cancel-jobs || true
make submit-job
sleep 8

echo "== [6/7] produce a burst (with malformed + duplicates) =="
make produce N=500 RATE=200 MALFORMED=5 DUP=10

echo "== [7/7] demo API =="
cat <<EOF

############################################################
#  Ready. Open:
#    Demo API : http://localhost:8000   (OpenAPI docs: /docs)
#    Flink UI : http://localhost:8081
#    MinIO    : http://localhost:9001   (admin / password)
#    Trino    : http://localhost:8080
#
#  Try:   make freshness        (latency percentiles)
#         curl localhost:8000/latest | python3 -m json.tool
############################################################

Starting the demo API (Ctrl-C to stop)...
EOF
make demo
