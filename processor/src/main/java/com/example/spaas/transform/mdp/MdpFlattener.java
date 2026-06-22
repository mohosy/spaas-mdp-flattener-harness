package com.example.spaas.transform.mdp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.spaas.api.QuarantineRecord;
import com.example.spaas.api.RawMessage;
import com.example.spaas.api.Transform;
import com.example.spaas.api.TransformResult;
import com.example.spaas.lineage.Hashing;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The MDP flattener: the first concrete {@link Transform}.
 *
 * Parses one envelope message, emits ONE output row per event with all business +
 * lineage columns, computes {@code canonical_row_hash} over the business columns,
 * and applies INTRA-MESSAGE deduplication by that hash (only within a single
 * message — never global). Bad input is routed to quarantine; this class never throws.
 *
 * Deterministic: identical input always yields identical business columns and
 * {@code canonical_row_hash}. {@code processed_at} can be pinned via the constructor
 * for fully deterministic output (used by tests); in production it is wall-clock now.
 */
public final class MdpFlattener implements Transform {
    private static final long serialVersionUID = 1L;

    private final MdpFieldMapping mapping;
    private final String processorVersion;
    /** If non-null, used for processed_at (test determinism); else Instant.now(). */
    private final Instant fixedProcessedAt;
    private transient ObjectMapper mapper;

    public MdpFlattener(MdpFieldMapping mapping, String processorVersion) {
        this(mapping, processorVersion, null);
    }

    public MdpFlattener(MdpFieldMapping mapping, String processorVersion, Instant fixedProcessedAt) {
        this.mapping = mapping;
        this.processorVersion = processorVersion;
        this.fixedProcessedAt = fixedProcessedAt;
    }

    @Override
    public String name() {
        return "mdp-flattener";
    }

    @Override
    public TransformResult apply(RawMessage msg) {
        List<Map<String, Object>> rows = new ArrayList<>();
        List<QuarantineRecord> quarantine = new ArrayList<>();

        byte[] value = msg.value != null ? msg.value : new byte[0];
        String rawWhole = new String(value, java.nio.charset.StandardCharsets.UTF_8);
        String payloadHash = Hashing.sha256Hex(value);
        Instant kafkaTs = Instant.ofEpochMilli(msg.timestampMs);
        Instant processedAt = fixedProcessedAt != null ? fixedProcessedAt : Instant.now();

        JsonNode root;
        try {
            root = mapper().readTree(value);
        } catch (Exception e) {
            quarantine.add(q(rawWhole, QuarantineRecord.Reason.MALFORMED_JSON,
                    e.getClass().getSimpleName() + ": " + e.getMessage(), msg));
            return new TransformResult(rows, quarantine, 0, 0);
        }

        JsonNode eventsNode = root == null ? null : root.get(mapping.eventsArrayField);
        if (eventsNode == null || !eventsNode.isArray()) {
            quarantine.add(q(rawWhole, QuarantineRecord.Reason.NOT_AN_ENVELOPE,
                    "missing or non-array events field '" + mapping.eventsArrayField + "'", msg));
            return new TransformResult(rows, quarantine, 0, 0);
        }

        String messageId = textOrNull(root, mapping.messageIdField);
        String schemaVersion = textOrNull(root, mapping.schemaVersionField);

        int inputEventCount = 0;
        int dedupDropped = 0;
        Set<String> seenInThisMessage = new HashSet<>();

        for (JsonNode ev : eventsNode) {
            inputEventCount++;

            String missing = firstMissingRequired(ev);
            if (missing != null) {
                quarantine.add(q(ev.toString(), QuarantineRecord.Reason.MISSING_REQUIRED_FIELD,
                        "missing or null required field '" + missing + "'", msg));
                continue;
            }

            Map<String, Object> business;
            try {
                business = buildBusiness(messageId, schemaVersion, ev);
            } catch (RuntimeException convEx) {
                quarantine.add(q(ev.toString(), QuarantineRecord.Reason.INVALID_FIELD_VALUE,
                        convEx.getMessage(), msg));
                continue;
            }

            String canonicalRowHash = Hashing.canonicalRowHash(business);
            if (!seenInThisMessage.add(canonicalRowHash)) {
                dedupDropped++;     // intra-message duplicate -> drop
                continue;
            }

            Map<String, Object> row = new LinkedHashMap<>(business);
            row.put("source_topic", msg.topic);
            row.put("source_partition", msg.partition);
            row.put("source_offset", msg.offset);
            row.put("kafka_timestamp", kafkaTs);
            row.put("raw_payload_hash", payloadHash);
            row.put("processor_version", processorVersion);
            row.put("canonical_row_hash", canonicalRowHash);
            row.put("processed_at", processedAt);
            rows.add(row);
        }

        return new TransformResult(rows, quarantine, inputEventCount, dedupDropped);
    }

    /**
     * Builds the business columns: first the direct columns, then any derived columns.
     * Direct numeric/timestamp columns throw on an invalid value (the existing strict
     * behavior, which routes the event to quarantine). The derived operations never throw;
     * they fall back to null or to a string, as each operation's contract requires.
     */
    private Map<String, Object> buildBusiness(String messageId, String schemaVersion, JsonNode ev) {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("message_id", messageId);
        b.put("schema_version", schemaVersion);
        for (Map.Entry<String, String> e : mapping.eventColumnToField.entrySet()) {
            String col = e.getKey();
            JsonNode node = ev.get(e.getValue());
            b.put(col, convert(col, node));
        }
        for (MdpOperation op : mapping.operations) {
            applyOperation(op, ev, b);
        }
        return b;
    }

    /** Dispatches one derived column rule to its implementation, writing into the row. */
    private void applyOperation(MdpOperation op, JsonNode ev, Map<String, Object> b) {
        if (op instanceof FallbackOp f) {
            b.put(f.column(), fallback(f, ev));
        } else if (op instanceof ArrayReduceOp a) {
            b.put(a.column(), arrayReduce(a, ev));
        } else if (op instanceof NumericStringSplitOp s) {
            numericStringSplit(s, ev, b);
        } else if (op instanceof MultiFormatTimestampOp t) {
            b.put(t.column(), multiFormatTimestamp(t, ev));
        } else {
            throw new IllegalStateException("unknown operation type: " + op.getClass());
        }
    }

    /** Returns the first source field that is present and not null (as text), else null. */
    private static Object fallback(FallbackOp op, JsonNode ev) {
        for (String source : op.sources()) {
            JsonNode n = ev.get(source);
            if (n != null && !n.isNull()) {
                return n.asText();
            }
        }
        return null;
    }

    /**
     * MAX or MIN over the numeric values of the array elements whose name matches the
     * target token (ignoring case). Values that are not numbers are skipped. Returns null
     * when nothing matches or nothing parses; with one match it returns that single value,
     * because the accumulator starts empty (null) and only then takes a value.
     */
    private static Object arrayReduce(ArrayReduceOp op, JsonNode ev) {
        JsonNode array = ev.get(op.arrayField());
        if (array == null || !array.isArray()) {
            return null;
        }
        Double acc = null;   // empty accumulator: the first kept value becomes the result
        for (JsonNode element : array) {
            JsonNode nameNode = element.get(op.nameKey());
            if (nameNode == null || !nameNode.asText().equalsIgnoreCase(op.matchToken())) {
                continue;
            }
            Double value = parseDecimalOrNull(textOf(element.get(op.valueKey())));
            if (value == null) {
                continue;   // skip a matched element whose value is not a number
            }
            acc = (acc == null) ? value
                    : (op.reduce() == ReduceKind.MAX ? Math.max(acc, value) : Math.min(acc, value));
        }
        return acc;
    }

    /**
     * Routes one source value to either the numeric column or the string column. A value
     * that parses as a decimal goes to the numeric column; otherwise the original text goes
     * to the string column. A present but non numeric value is kept as text, never
     * quarantined. A missing source leaves both columns null.
     */
    private static void numericStringSplit(NumericStringSplitOp op, JsonNode ev, Map<String, Object> b) {
        String text = textOf(ev.get(op.sourceField()));
        Double numeric = parseDecimalOrNull(text);
        b.put(op.numericColumn(), numeric);
        b.put(op.stringColumn(), numeric == null ? text : null);
    }

    /**
     * Parses the source string with the first accepted format that matches, else null. A
     * present but unparseable value is nulled rather than quarantined, matching the source
     * model behavior for optional timestamps.
     */
    private static Object multiFormatTimestamp(MultiFormatTimestampOp op, JsonNode ev) {
        String text = textOf(ev.get(op.sourceField()));
        if (text == null) {
            return null;
        }
        for (String format : op.formats()) {
            Instant parsed = tryTimestamp(format, text);
            if (parsed != null) {
                return parsed;
            }
        }
        return null;
    }

    /** Tries one timestamp format, returning null if the text does not match it. */
    private static Instant tryTimestamp(String format, String text) {
        try {
            if (MdpOperation.ISO_8601.equalsIgnoreCase(format)) {
                try {
                    return Instant.parse(text);                     // e.g. 2026-06-19T10:00:00.050Z
                } catch (Exception notInstant) {
                    return OffsetDateTime.parse(text).toInstant();  // e.g. ...+01:00
                }
            }
            // Any other format is a calendar date pattern, read at midnight UTC. This matches
            // the date only secondary formats the source model uses (for example MM/dd/yyyy).
            LocalDate date = LocalDate.parse(text, DateTimeFormatter.ofPattern(format, Locale.ROOT));
            return date.atStartOfDay(ZoneOffset.UTC).toInstant();
        } catch (Exception notThisFormat) {
            return null;
        }
    }

    /** Null-safe text of a JSON node: null for a missing or JSON-null node. */
    private static String textOf(JsonNode node) {
        return (node == null || node.isNull()) ? null : node.asText();
    }

    /** Parses text as a decimal, returning null (not an exception) when it is not a number. */
    private static Double parseDecimalOrNull(String text) {
        if (text == null) {
            return null;
        }
        try {
            return Double.parseDouble(text.trim());
        } catch (NumberFormatException nfe) {
            return null;
        }
    }

    private Object convert(String column, JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (mapping.numericColumns.contains(column)) {
            if (node.isNumber()) {
                return node.doubleValue();
            }
            try {
                return Double.parseDouble(node.asText());
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("column '" + column + "' is not numeric: " + node.asText());
            }
        }
        if (mapping.timestampColumns.contains(column)) {
            return parseInstant(column, node.asText());
        }
        return node.asText();
    }

    private static Instant parseInstant(String column, String text) {
        try {
            return Instant.parse(text);                 // e.g. 2026-06-19T10:00:00.050Z
        } catch (Exception ignore) {
            try {
                return OffsetDateTime.parse(text).toInstant(); // e.g. ...+00:00
            } catch (Exception e) {
                throw new IllegalArgumentException("column '" + column + "' is not a timestamp: " + text);
            }
        }
    }

    private String firstMissingRequired(JsonNode ev) {
        for (String field : mapping.requiredFields) {
            JsonNode n = ev.get(field);
            if (n == null || n.isNull()) {
                return field;
            }
        }
        return null;
    }

    private static String textOrNull(JsonNode parent, String field) {
        JsonNode n = parent.get(field);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private QuarantineRecord q(String rawPayload, String reason, String detail, RawMessage m) {
        return new QuarantineRecord(rawPayload, reason, detail, m.topic, m.partition, m.offset, m.timestampMs);
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }
}
