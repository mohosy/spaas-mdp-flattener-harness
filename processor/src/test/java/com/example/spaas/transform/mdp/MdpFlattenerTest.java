package com.example.spaas.transform.mdp;

import com.example.spaas.api.QuarantineRecord;
import com.example.spaas.api.RawMessage;
import com.example.spaas.api.TransformResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the MDP flattener (pure logic — no Flink runtime). */
class MdpFlattenerTest {

    private static final Instant FIXED = Instant.parse("2026-06-19T12:00:00Z");

    private MdpFlattener flattener() {
        return new MdpFlattener(MdpFieldMapping.defaultSynthetic(), "test-1.0", FIXED);
    }

    private RawMessage msg(String json, int partition, long offset) {
        return new RawMessage(json.getBytes(StandardCharsets.UTF_8), "mdp.mfg.raw", partition, offset, 1_000L);
    }

    private String event(String eventId, String serial, double value) {
        return "{"
                + "\"eventId\":\"" + eventId + "\","
                + "\"testStationId\":\"EFT-07\",\"productModel\":\"RX-5\","
                + "\"productSerial\":\"" + serial + "\",\"testProgram\":\"FinalFunctional\","
                + "\"testSessionId\":\"sess-1\",\"operatorId\":\"op-42\","
                + "\"measurementName\":\"motor_torque_axis1\",\"measurementValue\":" + value + ","
                + "\"measurementUnit\":\"Nm\",\"lowerLimit\":10.0,\"upperLimit\":15.0,"
                + "\"result\":\"PASS\",\"measuredAt\":\"2026-06-19T10:00:00.050Z\"}";
    }

    private String envelope(String messageId, String... events) {
        return "{\"messageId\":\"" + messageId + "\",\"schemaVersion\":\"mdp.mfg.v1\","
                + "\"producedAt\":\"2026-06-19T10:00:00.123Z\",\"events\":[" + String.join(",", events) + "]}";
    }

    @Test
    void happyPath_manyEvents_manyRows_oneRowPerEvent() {
        String json = envelope("m1", event("e1", "SN-1", 12.3), event("e2", "SN-2", 11.0), event("e3", "SN-3", 9.9));
        TransformResult r = flattener().apply(msg(json, 0, 5));

        assertThat(r.flattenedRows).hasSize(3);          // one row per event
        assertThat(r.quarantined).isEmpty();
        assertThat(r.inputEventCount).isEqualTo(3);
        assertThat(r.dedupDropped).isEqualTo(0);

        Map<String, Object> row = r.flattenedRows.get(0);
        assertThat(row.get("message_id")).isEqualTo("m1");
        assertThat(row.get("schema_version")).isEqualTo("mdp.mfg.v1");
        assertThat(row.get("event_id")).isEqualTo("e1");
        assertThat(row.get("measurement_value")).isEqualTo(12.3);   // numeric -> Double
        assertThat(row.get("measured_at")).isInstanceOf(Instant.class);
        // lineage
        assertThat(row.get("source_topic")).isEqualTo("mdp.mfg.raw");
        assertThat(row.get("source_partition")).isEqualTo(0);
        assertThat(row.get("source_offset")).isEqualTo(5L);
        assertThat(row.get("processor_version")).isEqualTo("test-1.0");
        assertThat(row.get("canonical_row_hash")).asString().hasSize(64); // sha256 hex
        assertThat(row.get("processed_at")).isEqualTo(FIXED);
    }

    @Test
    void deterministicCanonicalRowHash() {
        String json = envelope("m1", event("e1", "SN-1", 12.34));
        // same input via two independent flattener instances + different offset/partition
        Object h1 = flattener().apply(msg(json, 0, 1)).flattenedRows.get(0).get("canonical_row_hash");
        Object h2 = flattener().apply(msg(json, 2, 999)).flattenedRows.get(0).get("canonical_row_hash");
        assertThat(h1).isEqualTo(h2);   // hash depends only on business columns, not lineage
    }

    @Test
    void intraMessageDedup_dropsDuplicatesWithinMessage() {
        // identical event repeated within ONE message
        String dup = event("e1", "SN-1", 12.34);
        String json = envelope("m1", dup, dup, event("e2", "SN-2", 13.0));
        TransformResult r = flattener().apply(msg(json, 0, 0));

        assertThat(r.inputEventCount).isEqualTo(3);
        assertThat(r.flattenedRows).hasSize(2);     // the duplicate collapsed
        assertThat(r.dedupDropped).isEqualTo(1);
    }

    @Test
    void malformedJson_quarantined_noCrash() {
        TransformResult r = flattener().apply(msg("{ this is not json %%%", 1, 7));
        assertThat(r.flattenedRows).isEmpty();
        assertThat(r.quarantined).hasSize(1);
        QuarantineRecord q = r.quarantined.get(0);
        assertThat(q.errorReason).isEqualTo(QuarantineRecord.Reason.MALFORMED_JSON);
        assertThat(q.partition).isEqualTo(1);
    }

    @Test
    void missingRequiredField_quarantined_validEventsStillFlow() {
        // event missing productSerial (required) alongside a valid event in same message
        String bad = "{\"eventId\":\"e1\",\"measurementName\":\"x\",\"measurementValue\":1.0,"
                + "\"measuredAt\":\"2026-06-19T10:00:00.050Z\"}";   // no productSerial
        String json = envelope("m1", bad, event("e2", "SN-2", 5.0));
        TransformResult r = flattener().apply(msg(json, 0, 0));

        assertThat(r.flattenedRows).hasSize(1);                       // the good event
        assertThat(r.flattenedRows.get(0).get("event_id")).isEqualTo("e2");
        assertThat(r.quarantined).hasSize(1);
        assertThat(r.quarantined.get(0).errorReason).isEqualTo(QuarantineRecord.Reason.MISSING_REQUIRED_FIELD);
        assertThat(r.quarantined.get(0).errorDetail).contains("productSerial");
        assertThat(r.inputEventCount).isEqualTo(2);
    }

    @Test
    void missingEventsArray_quarantinedAsNotAnEnvelope() {
        TransformResult r = flattener().apply(msg("{\"messageId\":\"m1\",\"schemaVersion\":\"v\"}", 0, 0));
        assertThat(r.flattenedRows).isEmpty();
        assertThat(r.quarantined).hasSize(1);
        assertThat(r.quarantined.get(0).errorReason).isEqualTo(QuarantineRecord.Reason.NOT_AN_ENVELOPE);
    }

    @Test
    void invalidNumericValue_quarantined() {
        String bad = "{\"eventId\":\"e1\",\"productSerial\":\"SN-1\",\"measurementName\":\"x\","
                + "\"measurementValue\":\"not-a-number\",\"measuredAt\":\"2026-06-19T10:00:00.050Z\"}";
        TransformResult r = flattener().apply(msg(envelope("m1", bad), 0, 0));
        assertThat(r.flattenedRows).isEmpty();
        assertThat(r.quarantined).hasSize(1);
        assertThat(r.quarantined.get(0).errorReason).isEqualTo(QuarantineRecord.Reason.INVALID_FIELD_VALUE);
    }

    // ---------------------------------------------------------------------------------
    // Derived columns: the four operations defined in MdpFieldMapping.defaultSynthetic().
    // Each test feeds an otherwise valid event that also carries the operation's source
    // fields, then asserts the single flattened row's derived columns.
    // ---------------------------------------------------------------------------------

    /** A valid event (passes the required fields) plus the given extra JSON members. */
    private String eventWithExtras(String extraJson) {
        String base = "\"eventId\":\"e1\",\"productSerial\":\"SN-1\","
                + "\"measurementName\":\"torque\",\"measurementValue\":12.0,"
                + "\"measuredAt\":\"2026-06-19T10:00:00.050Z\"";
        return "{" + base + (extraJson.isBlank() ? "" : "," + extraJson) + "}";
    }

    /** Flattens one valid event carrying the extra fields and returns its single row. */
    private Map<String, Object> rowWithExtras(String extraJson) {
        TransformResult r = flattener().apply(msg(envelope("m1", eventWithExtras(extraJson)), 0, 0));
        assertThat(r.quarantined).isEmpty();
        assertThat(r.flattenedRows).hasSize(1);
        return r.flattenedRows.get(0);
    }

    @Test
    void fallbackChain_takesFirstPresentSource() {
        // instrument present -> wins over later candidates
        assertThat(rowWithExtras("\"instrumentId\":\"INS-1\",\"accessoryId\":\"ACC-2\"")
                .get("product_identity")).isEqualTo("INS-1");
    }

    @Test
    void fallbackChain_fallsThroughNullAndMissingToLaterSource() {
        // instrument is JSON null, accessory missing, sub assembly present -> third wins
        assertThat(rowWithExtras("\"instrumentId\":null,\"subAssemblyId\":\"SUB-3\"")
                .get("product_identity")).isEqualTo("SUB-3");
    }

    @Test
    void fallbackChain_allCandidatesNull_isNull() {
        // none of instrumentId/accessoryId/subAssemblyId present
        assertThat(rowWithExtras("").get("product_identity")).isNull();
    }

    @Test
    void arrayReduce_maxAndMin_overNamedEntries() {
        Map<String, Object> row = rowWithExtras("\"testSpecAttributes\":["
                + "{\"name\":\"UL\",\"value\":\"15.0\"},{\"name\":\"UL\",\"value\":\"16.5\"},"
                + "{\"name\":\"LL\",\"value\":\"9.0\"}]");
        assertThat(row.get("spec_upper_limit")).isEqualTo(16.5);   // MAX of the UL entries
        assertThat(row.get("spec_lower_limit")).isEqualTo(9.0);    // MIN of the LL entries
    }

    @Test
    void arrayReduce_skipsNonNumericValues() {
        Map<String, Object> row = rowWithExtras("\"testSpecAttributes\":["
                + "{\"name\":\"UL\",\"value\":\"abc\"},{\"name\":\"UL\",\"value\":\"16.5\"}]");
        assertThat(row.get("spec_upper_limit")).isEqualTo(16.5);   // "abc" skipped, not quarantined
    }

    @Test
    void arrayReduce_singleMatch_caseInsensitive_returnsThatValue() {
        // lower case name still matches "UL"; one value must come back, not null
        Map<String, Object> row = rowWithExtras(
                "\"testSpecAttributes\":[{\"name\":\"ul\",\"value\":\"12.5\"}]");
        assertThat(row.get("spec_upper_limit")).isEqualTo(12.5);
    }

    @Test
    void arrayReduce_noMatchingElement_isNull() {
        Map<String, Object> row = rowWithExtras(
                "\"testSpecAttributes\":[{\"name\":\"OTHER\",\"value\":\"1.0\"}]");
        assertThat(row.get("spec_upper_limit")).isNull();
        assertThat(row.get("spec_lower_limit")).isNull();
    }

    @Test
    void numericStringSplit_numericValue_goesToNumericColumn() {
        Map<String, Object> row = rowWithExtras("\"measurementResult\":\"12.5\"");
        assertThat(row.get("measurement_result_numeric")).isEqualTo(12.5);
        assertThat(row.get("measurement_result_text")).isNull();
    }

    @Test
    void numericStringSplit_textValue_goesToStringColumn_notQuarantined() {
        Map<String, Object> row = rowWithExtras("\"measurementResult\":\"RETEST\"");
        assertThat(row.get("measurement_result_numeric")).isNull();
        assertThat(row.get("measurement_result_text")).isEqualTo("RETEST");
    }

    @Test
    void multiFormatTimestamp_parsesEachAcceptedFormat() {
        // ISO 8601 instant
        assertThat(rowWithExtras("\"calibratedAt\":\"2026-06-19T10:00:00.050Z\"").get("calibrated_at"))
                .isEqualTo(Instant.parse("2026-06-19T10:00:00.050Z"));
        // MM/dd/yyyy, read at midnight UTC
        assertThat(rowWithExtras("\"calibratedAt\":\"06/19/2026\"").get("calibrated_at"))
                .isEqualTo(Instant.parse("2026-06-19T00:00:00Z"));
        // yyyy-MM-dd, read at midnight UTC
        assertThat(rowWithExtras("\"calibratedAt\":\"2026-06-19\"").get("calibrated_at"))
                .isEqualTo(Instant.parse("2026-06-19T00:00:00Z"));
    }

    @Test
    void multiFormatTimestamp_unparseableValue_isNull() {
        assertThat(rowWithExtras("\"calibratedAt\":\"not-a-date\"").get("calibrated_at")).isNull();
    }

    @Test
    void derivedColumns_areDeterministic() {
        String extras = "\"instrumentId\":\"INS-1\",\"measurementResult\":\"7.25\","
                + "\"calibratedAt\":\"06/19/2026\",\"testSpecAttributes\":["
                + "{\"name\":\"UL\",\"value\":\"16.5\"},{\"name\":\"LL\",\"value\":\"9.0\"}]";
        Map<String, Object> a = rowWithExtras(extras);
        Map<String, Object> b = rowWithExtras(extras);
        for (String col : List.of("product_identity", "spec_upper_limit", "spec_lower_limit",
                "measurement_result_numeric", "calibrated_at", "canonical_row_hash")) {
            assertThat(a.get(col)).as(col).isEqualTo(b.get(col));
        }
    }
}
