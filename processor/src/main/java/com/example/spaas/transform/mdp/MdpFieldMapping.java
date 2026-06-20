package com.example.spaas.transform.mdp;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
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

    public MdpFieldMapping(String messageIdField, String schemaVersionField, String eventsArrayField,
                           LinkedHashMap<String, String> eventColumnToField, List<String> requiredFields,
                           Set<String> numericColumns, Set<String> timestampColumns) {
        this.messageIdField = messageIdField;
        this.schemaVersionField = schemaVersionField;
        this.eventsArrayField = eventsArrayField;
        this.eventColumnToField = eventColumnToField;
        this.requiredFields = requiredFields;
        this.numericColumns = numericColumns;
        this.timestampColumns = timestampColumns;
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

        return new MdpFieldMapping(
                String.valueOf(env.get("message_id")),
                String.valueOf(env.get("schema_version")),
                String.valueOf(env.get("events_array")),
                eventCols, required, numeric, timestamps);
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
        return new MdpFieldMapping(
                "messageId", "schemaVersion", "events",
                cols,
                List.of("eventId", "productSerial", "measurementName", "measurementValue", "measuredAt"),
                new LinkedHashSet<>(List.of("measurement_value", "lower_limit", "upper_limit")),
                new LinkedHashSet<>(List.of("measured_at")));
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
