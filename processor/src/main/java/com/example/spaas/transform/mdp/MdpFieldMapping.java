package com.example.spaas.transform.mdp;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The pluggable manufacturing-schema mapping — the in-code counterpart of
 * config/field-mapping.yaml (REPLACE WITH REAL SCHEMA). Tells {@link MdpFlattener}
 * how to read the envelope/event JSON and which fields are required/numeric/timestamps.
 *
 * Loaded once on the client and serialized into the Flink operator (so task
 * managers don't re-read the file). To swap in the real event shape, edit the
 * YAML — this class and the flattener stay the same.
 */
public final class MdpFieldMapping implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String messageIdField;       // envelope -> message_id
    public final String schemaVersionField;   // envelope -> schema_version
    public final String eventsArrayField;     // envelope array of events
    /** business_column -> json field within each event (the 14 event-level columns). */
    public final LinkedHashMap<String, String> eventColumnToField;
    /** Required fields by JSON name within an event. Missing/null -> quarantine. */
    public final List<String> requiredFields;
    /** Business columns parsed as doubles. */
    public final Set<String> numericColumns;
    /** Business columns parsed as ISO-8601 timestamps. */
    public final Set<String> timestampColumns;
    /**
     * Derived columns computed by one of the four operations (see {@link MdpOperation}).
     * Applied after the direct columns, in order. Empty when the mapping uses only direct
     * columns, which keeps every existing mapping valid.
     */
    public final List<MdpOperation> operations;

    public MdpFieldMapping(String messageIdField, String schemaVersionField, String eventsArrayField,
                           LinkedHashMap<String, String> eventColumnToField, List<String> requiredFields,
                           Set<String> numericColumns, Set<String> timestampColumns,
                           List<MdpOperation> operations) {
        this.messageIdField = messageIdField;
        this.schemaVersionField = schemaVersionField;
        this.eventsArrayField = eventsArrayField;
        this.eventColumnToField = eventColumnToField;
        this.requiredFields = requiredFields;
        this.numericColumns = numericColumns;
        this.timestampColumns = timestampColumns;
        this.operations = operations;
    }

    public static MdpFieldMapping fromYamlFile(String path) {
        try (InputStream in = new FileInputStream(path)) {
            Object root = new Yaml().load(in);
            if (!(root instanceof Map)) {
                throw new IllegalArgumentException("field-mapping.yaml did not parse to a map: " + path);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) root;
            return fromYaml(m);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load field mapping from " + path, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static MdpFieldMapping fromYaml(Map<String, Object> root) {
        Map<String, Object> env = asMap(root.get("envelope"), "envelope");
        Map<String, Object> event = asMap(root.get("event"), "event");

        LinkedHashMap<String, String> eventCols = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : event.entrySet()) {
            eventCols.put(e.getKey(), String.valueOf(e.getValue()));
        }
        List<String> required = (List<String>) root.getOrDefault("required_fields", List.of());
        Set<String> numeric = new LinkedHashSet<>((List<String>) root.getOrDefault("numeric_fields", List.of()));
        Set<String> timestamps = new LinkedHashSet<>((List<String>) root.getOrDefault("timestamp_fields", List.of()));
        List<MdpOperation> operations = parseOperations(root.get("derived"));

        return new MdpFieldMapping(
                String.valueOf(env.get("message_id")),
                String.valueOf(env.get("schema_version")),
                String.valueOf(env.get("events_array")),
                eventCols, required, numeric, timestamps, operations);
    }

    /** Parses the optional {@code derived:} list into typed operations. */
    @SuppressWarnings("unchecked")
    private static List<MdpOperation> parseOperations(Object derived) {
        if (derived == null) {
            return List.of();   // derived columns are optional; mappings without them stay valid
        }
        if (!(derived instanceof List)) {
            throw new IllegalArgumentException("field mapping 'derived' must be a list of operations");
        }
        List<MdpOperation> ops = new ArrayList<>();
        for (Object item : (List<Object>) derived) {
            if (!(item instanceof Map)) {
                throw new IllegalArgumentException("each 'derived' entry must be a map with an 'op' key");
            }
            ops.add(parseOperation((Map<String, Object>) item));
        }
        return List.copyOf(ops);
    }

    /** Builds one operation from its YAML map, choosing the type by the {@code op} key. */
    private static MdpOperation parseOperation(Map<String, Object> m) {
        String op = str(m.get("op"), "op");
        switch (op) {
            case "fallback":
                return new FallbackOp(str(m.get("column"), "column"), strList(m.get("sources"), "sources"));
            case "array_reduce":
                return new ArrayReduceOp(str(m.get("column"), "column"),
                        str(m.get("source_array"), "source_array"),
                        optStr(m.get("name_key"), "name"),
                        optStr(m.get("value_key"), "value"),
                        str(m.get("match"), "match"),
                        reduceKind(m.get("reduce")));
            case "numeric_string_split":
                return new NumericStringSplitOp(str(m.get("source"), "source"),
                        str(m.get("numeric_column"), "numeric_column"),
                        str(m.get("string_column"), "string_column"));
            case "multi_format_timestamp":
                return new MultiFormatTimestampOp(str(m.get("column"), "column"),
                        str(m.get("source"), "source"),
                        strList(m.get("formats"), "formats"));
            default:
                throw new IllegalArgumentException("unknown derived op '" + op + "'");
        }
    }

    private static String str(Object v, String key) {
        if (v == null || String.valueOf(v).isBlank()) {
            throw new IllegalArgumentException("derived op missing required key '" + key + "'");
        }
        return String.valueOf(v);
    }

    private static String optStr(Object v, String def) {
        return (v == null || String.valueOf(v).isBlank()) ? def : String.valueOf(v);
    }

    @SuppressWarnings("unchecked")
    private static List<String> strList(Object v, String key) {
        if (!(v instanceof List) || ((List<Object>) v).isEmpty()) {
            throw new IllegalArgumentException("derived op key '" + key + "' must be a non empty list");
        }
        List<String> out = new ArrayList<>();
        for (Object o : (List<Object>) v) {
            out.add(String.valueOf(o));
        }
        return out;
    }

    private static ReduceKind reduceKind(Object v) {
        String s = str(v, "reduce").trim().toUpperCase(Locale.ROOT);
        if (s.equals("MAX")) {
            return ReduceKind.MAX;
        }
        if (s.equals("MIN")) {
            return ReduceKind.MIN;
        }
        throw new IllegalArgumentException("array_reduce 'reduce' must be max or min, got '" + v + "'");
    }

    /** Default synthetic mapping (mirrors config/field-mapping.yaml) — for unit tests. */
    public static MdpFieldMapping defaultSynthetic() {
        LinkedHashMap<String, String> cols = new LinkedHashMap<>();
        cols.put("event_id", "eventId");
        cols.put("test_station_id", "testStationId");
        cols.put("product_model", "productModel");
        cols.put("product_serial", "productSerial");
        cols.put("test_program", "testProgram");
        cols.put("test_session_id", "testSessionId");
        cols.put("operator_id", "operatorId");
        cols.put("measurement_name", "measurementName");
        cols.put("measurement_value", "measurementValue");
        cols.put("measurement_unit", "measurementUnit");
        cols.put("lower_limit", "lowerLimit");
        cols.put("upper_limit", "upperLimit");
        cols.put("result", "result");
        cols.put("measured_at", "measuredAt");
        // One clear example of each derived operation, mirroring config/field-mapping.yaml.
        // These reference real MDP shapes that the synthetic generator does not emit, so on
        // the live synthetic path they evaluate to null and the flattened table is unchanged;
        // the unit tests exercise them with crafted JSON that does carry these fields.
        List<MdpOperation> operations = List.of(
                new FallbackOp("product_identity", List.of("instrumentId", "accessoryId", "subAssemblyId")),
                new ArrayReduceOp("spec_upper_limit", "testSpecAttributes", "name", "value", "UL", ReduceKind.MAX),
                new ArrayReduceOp("spec_lower_limit", "testSpecAttributes", "name", "value", "LL", ReduceKind.MIN),
                new NumericStringSplitOp("measurementResult", "measurement_result_numeric", "measurement_result_text"),
                new MultiFormatTimestampOp("calibrated_at", "calibratedAt",
                        List.of(MdpOperation.ISO_8601, "MM/dd/yyyy", "yyyy-MM-dd")));
        return new MdpFieldMapping(
                "messageId", "schemaVersion", "events",
                cols,
                List.of("eventId", "productSerial", "measurementName", "measurementValue", "measuredAt"),
                new LinkedHashSet<>(List.of("measurement_value", "lower_limit", "upper_limit")),
                new LinkedHashSet<>(List.of("measured_at")),
                operations);
    }

    private static Map<String, Object> asMap(Object o, String name) {
        if (!(o instanceof Map)) {
            throw new IllegalArgumentException("field mapping section '" + name + "' missing or not a map");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> m = (Map<String, Object>) o;
        return m;
    }
}
