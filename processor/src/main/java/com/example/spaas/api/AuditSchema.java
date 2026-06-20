package com.example.spaas.api;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.Instant;
import java.util.List;

/**
 * Schema for {@code mdp.audit} — one row per checkpoint/commit window with run
 * metadata. Must match scripts/sql/create_tables.sql.
 *
 * {@code partition_offsets} is a JSON string {@code {"0":{"min":..,"max":..},...}}
 * (per-partition min/max offset seen in the window). {@code snapshot_id} is the
 * flattened table's Iceberg snapshot id if obtainable, else null.
 */
public final class AuditSchema {

    public static final List<String> COLUMNS = List.of(
            "run_id", "processor_version", "topic", "partition_offsets",
            "input_event_count", "output_row_count", "quarantine_count", "dedup_dropped_count",
            "commit_timestamp", "snapshot_id");

    public static final RowType ROW_TYPE = RowType.of(
            new LogicalType[]{
                    new VarCharType(VarCharType.MAX_LENGTH), // run_id
                    new VarCharType(VarCharType.MAX_LENGTH), // processor_version
                    new VarCharType(VarCharType.MAX_LENGTH), // topic
                    new VarCharType(VarCharType.MAX_LENGTH), // partition_offsets (JSON)
                    new BigIntType(),                        // input_event_count
                    new BigIntType(),                        // output_row_count
                    new BigIntType(),                        // quarantine_count
                    new BigIntType(),                        // dedup_dropped_count
                    new LocalZonedTimestampType(6),          // commit_timestamp
                    new BigIntType().copy(true)              // snapshot_id (nullable)
            },
            COLUMNS.toArray(new String[0]));

    private AuditSchema() {}

    public static RowData toRowData(String runId, String processorVersion, String topic,
                                    String partitionOffsetsJson, long inputEventCount,
                                    long outputRowCount, long quarantineCount, long dedupDropped,
                                    Instant commitTs, Long snapshotId) {
        GenericRowData r = new GenericRowData(COLUMNS.size());
        r.setField(0, StringData.fromString(runId));
        r.setField(1, StringData.fromString(processorVersion));
        r.setField(2, StringData.fromString(topic));
        r.setField(3, StringData.fromString(partitionOffsetsJson));
        r.setField(4, inputEventCount);
        r.setField(5, outputRowCount);
        r.setField(6, quarantineCount);
        r.setField(7, dedupDropped);
        r.setField(8, TimestampData.fromInstant(commitTs));
        r.setField(9, snapshotId);   // may be null
        return r;
    }
}
