#!/usr/bin/env bash
# M0 smoke test: prove the Kafka-less path works end to end at the catalog layer.
# Creates an Iceberg namespace + empty table through Trino (which writes metadata
# to the REST catalog and data files to MinIO), queries it for zero rows, then
# drops it. If this passes, Trino <-> REST catalog <-> MinIO are all wired.
set -euo pipefail
TR="bash scripts/trino.sh -e"

echo "== M0 smoke test =="
echo "-- create namespace mdp"
$TR "CREATE SCHEMA IF NOT EXISTS iceberg.mdp"

echo "-- create empty table iceberg.mdp.smoke"
$TR "CREATE TABLE IF NOT EXISTS iceberg.mdp.smoke (id bigint, note varchar)"

echo "-- count rows (expect 0)"
count=$($TR "SELECT count(*) FROM iceberg.mdp.smoke" | tr -d '[:space:]')
echo "   row count = '${count}'"

echo "-- list tables in mdp"
$TR "SHOW TABLES FROM iceberg.mdp"

echo "-- drop smoke table"
$TR "DROP TABLE iceberg.mdp.smoke"

if [ "$count" = "0" ]; then
  echo "== M0 SMOKE PASS: empty Iceberg table created + queried (0 rows) =="
else
  echo "== M0 SMOKE FAIL: expected 0 rows, got '${count}' =="; exit 1
fi
