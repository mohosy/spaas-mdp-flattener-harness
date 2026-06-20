package com.example.spaas.api;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Single source of truth for the {@code mdp.flattened_measurements} output schema.
 * Column order and types here MUST match {@code scripts/sql/create_tables.sql}.
 *
 * Columns = 16 BUSINESS columns (used to compute {@code canonical_row_hash}) followed
 * by lineage-only columns. {@link #toRowData(Map)} converts a column-name -&gt; value
 * map into Iceberg-shaped {@link RowData}, so a new transform only has to emit the
 * right map — no RowData plumbing.
 */
public final class FlattenedSchema {

    enum Kind { STRING, DOUBLE, INT, LONG, TIMESTAMPTZ }

    static final class Column {
        final String name;
        final Kind kind;
        Column(String name, Kind kind) { this.name = name; this.kind = kind; }
    }

    private static Column s(String n) { return new Column(n, Kind.STRING); }
    private static Column d(String n) { return new Column(n, Kind.DOUBLE); }
    private static Column ts(String n) { return new Column(n, Kind.TIMESTAMPTZ); }

    /** The 16 business columns. canonical_row_hash is computed over exactly these. */
    public static final List<String> BUSINESS_COLUMNS = List.of(
            "message_id", "schema_version", "event_id", "test_station_id", "product_model",
            "product_serial", "test_program", "test_session_id", "operator_id",
            "measurement_name", "measurement_value", "measurement_unit", "lower_limit",
            "upper_limit", "result", "measured_at");

    private static final List<Column> COLUMNS = List.of(
            // --- business (16) ---
            s("message_id"), s("schema_version"), s("event_id"), s("test_station_id"),
            s("product_model"), s("product_serial"), s("test_program"), s("test_session_id"),
            s("operator_id"), s("measurement_name"), d("measurement_value"), s("measurement_unit"),
            d("lower_limit"), d("upper_limit"), s("result"), ts("measured_at"),
            // --- lineage (only those not already a business column; schema_version is shared) ---
            s("source_topic"), new Column("source_partition", Kind.INT),
            new Column("source_offset", Kind.LONG), ts("kafka_timestamp"),
            s("raw_payload_hash"), s("processor_version"), s("canonical_row_hash"),
            ts("processed_at"));

    public static final List<String> ALL_COLUMNS;
    public static final RowType ROW_TYPE;

    static {
        List<String> names = new ArrayList<>(COLUMNS.size());
        LogicalType[] types = new LogicalType[COLUMNS.size()];
        for (int i = 0; i < COLUMNS.size(); i++) {
            names.add(COLUMNS.get(i).name);
            types[i] = logicalType(COLUMNS.get(i).kind);
        }
        ALL_COLUMNS = List.copyOf(names);
        ROW_TYPE = RowType.of(types, names.toArray(new String[0]));
    }

    private FlattenedSchema() {}

    private static LogicalType logicalType(Kind k) {
        switch (k) {
            case STRING:      return new VarCharType(VarCharType.MAX_LENGTH);
            case DOUBLE:      return new DoubleType();
            case INT:         return new IntType();
            case LONG:        return new BigIntType();
            case TIMESTAMPTZ: return new LocalZonedTimestampType(6);
            default: throw new IllegalStateException("unknown kind " + k);
        }
    }

    /** Convert a column-name -&gt; value map into RowData in {@link #ALL_COLUMNS} order. */
    public static RowData toRowData(Map<String, Object> row) {
        GenericRowData out = new GenericRowData(COLUMNS.size());
        for (int i = 0; i < COLUMNS.size(); i++) {
            Column c = COLUMNS.get(i);
            out.setField(i, convert(c, row.get(c.name)));
        }
        return out;
    }

    private static Object convert(Column c, Object v) {
        if (v == null) {
            return null;
        }
        switch (c.kind) {
            case STRING:      return StringData.fromString(v.toString());
            case DOUBLE:      return ((Number) v).doubleValue();
            case INT:         return ((Number) v).intValue();
            case LONG:        return ((Number) v).longValue();
            case TIMESTAMPTZ: return TimestampData.fromInstant((Instant) v);
            default: throw new IllegalStateException("unknown kind " + c.kind);
        }
    }

    /** Column names as a SQL-ish list, for docs/sanity (not used at runtime). */
    public static String columnsSql() {
        return String.join(", ", ALL_COLUMNS);
    }

    static List<String> columnKinds() {
        List<String> ks = new ArrayList<>();
        for (Column c : COLUMNS) ks.add(c.name + ":" + c.kind);
        return ks;
    }

    static String[] businessColumnsArray() {
        return BUSINESS_COLUMNS.toArray(new String[0]);
    }

    static {
        // Sanity: business columns must be a prefix of ALL_COLUMNS.
        if (!ALL_COLUMNS.subList(0, BUSINESS_COLUMNS.size()).equals(BUSINESS_COLUMNS)) {
            throw new IllegalStateException("BUSINESS_COLUMNS must be the prefix of ALL_COLUMNS");
        }
        Arrays.hashCode(new int[0]); // no-op to keep static-init block legal/grouped
    }
}
