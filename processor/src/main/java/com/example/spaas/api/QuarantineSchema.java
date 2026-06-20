package com.example.spaas.api;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.Instant;
import java.util.List;

/** Schema for {@code mdp.quarantine}. Must match scripts/sql/create_tables.sql. */
public final class QuarantineSchema {

    public static final List<String> COLUMNS = List.of(
            "raw_payload", "error_reason", "error_detail",
            "source_topic", "source_partition", "source_offset",
            "kafka_timestamp", "ingested_at");

    public static final RowType ROW_TYPE = RowType.of(
            new LogicalType[]{
                    new VarCharType(VarCharType.MAX_LENGTH), // raw_payload
                    new VarCharType(VarCharType.MAX_LENGTH), // error_reason
                    new VarCharType(VarCharType.MAX_LENGTH), // error_detail
                    new VarCharType(VarCharType.MAX_LENGTH), // source_topic
                    new IntType(),                           // source_partition
                    new BigIntType(),                        // source_offset
                    new LocalZonedTimestampType(6),          // kafka_timestamp
                    new LocalZonedTimestampType(6)           // ingested_at
            },
            COLUMNS.toArray(new String[0]));

    private QuarantineSchema() {}

    public static RowData toRowData(QuarantineRecord q, Instant ingestedAt) {
        GenericRowData r = new GenericRowData(COLUMNS.size());
        r.setField(0, sd(q.rawPayload));
        r.setField(1, sd(q.errorReason));
        r.setField(2, sd(q.errorDetail));
        r.setField(3, sd(q.topic));
        r.setField(4, q.partition);
        r.setField(5, q.offset);
        r.setField(6, TimestampData.fromEpochMillis(q.timestampMs));
        r.setField(7, TimestampData.fromInstant(ingestedAt));
        return r;
    }

    private static StringData sd(String s) {
        return s == null ? null : StringData.fromString(s);
    }
}
