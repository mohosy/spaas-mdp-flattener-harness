package com.example.spaas.transform.mdp;

import com.example.spaas.api.QuarantineRecord;
import com.example.spaas.api.RawMessage;
import com.example.spaas.api.TransformResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
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
}
