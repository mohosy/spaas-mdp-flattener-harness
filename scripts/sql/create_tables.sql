-- Iceberg DDL for the SPaaS MDP harness, applied through Trino (the external
-- query engine). The processor writes RowData whose column order/types match
-- these tables exactly. In production these tables are "externally owned"; here
-- we own them via this single DDL file. Run with: make create-tables
--
-- Column types map: Trino timestamp(6) with time zone <-> Iceberg timestamptz
-- <-> Flink TIMESTAMP_LTZ(6). varchar <-> Iceberg string. integer/bigint/double direct.

CREATE SCHEMA IF NOT EXISTS iceberg.mdp;

-- M1: one near-passthrough row per Kafka MESSAGE (raw payload + basic lineage).
CREATE TABLE IF NOT EXISTS iceberg.mdp.raw_events (
  message_id        varchar,
  schema_version    varchar,
  raw_payload       varchar,
  source_topic      varchar,
  source_partition  integer,
  source_offset     bigint,
  kafka_timestamp   timestamp(6) with time zone,
  raw_payload_hash  varchar,
  processor_version varchar,
  processed_at      timestamp(6) with time zone
) WITH (format = 'PARQUET');

-- M2: one flattened row per EVENT. 16 business columns + lineage.
-- Column order/types MUST match com.example.spaas.api.FlattenedSchema.
CREATE TABLE IF NOT EXISTS iceberg.mdp.flattened_measurements (
  message_id         varchar,
  schema_version     varchar,
  event_id           varchar,
  test_station_id    varchar,
  product_model      varchar,
  product_serial     varchar,
  test_program       varchar,
  test_session_id    varchar,
  operator_id        varchar,
  measurement_name   varchar,
  measurement_value  double,
  measurement_unit   varchar,
  lower_limit        double,
  upper_limit        double,
  result             varchar,
  measured_at        timestamp(6) with time zone,
  source_topic       varchar,
  source_partition   integer,
  source_offset      bigint,
  kafka_timestamp    timestamp(6) with time zone,
  raw_payload_hash   varchar,
  processor_version  varchar,
  canonical_row_hash varchar,
  processed_at       timestamp(6) with time zone
) WITH (format = 'PARQUET');

-- M3: quarantine for bad records. Matches com.example.spaas.api.QuarantineSchema.
CREATE TABLE IF NOT EXISTS iceberg.mdp.quarantine (
  raw_payload      varchar,
  error_reason     varchar,
  error_detail     varchar,
  source_topic     varchar,
  source_partition integer,
  source_offset    bigint,
  kafka_timestamp  timestamp(6) with time zone,
  ingested_at      timestamp(6) with time zone
) WITH (format = 'PARQUET');

-- M3: audit, one row per checkpoint/commit window. Matches com.example.spaas.api.AuditSchema.
CREATE TABLE IF NOT EXISTS iceberg.mdp.audit (
  run_id              varchar,
  processor_version   varchar,
  topic               varchar,
  partition_offsets   varchar,
  input_event_count   bigint,
  output_row_count    bigint,
  quarantine_count    bigint,
  dedup_dropped_count bigint,
  commit_timestamp    timestamp(6) with time zone,
  snapshot_id         bigint
) WITH (format = 'PARQUET');
