package com.example.spaas.transform.mdp;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the field mapping YAML parser, focusing on the four derived operations: that the
 * format in config/field-mapping.yaml parses into the expected typed operations, and that
 * a misconfigured operation fails fast with a clear message when the mapping is loaded.
 */
class MdpFieldMappingTest {

    /** Minimal envelope/event header so {@link MdpFieldMapping#fromYaml} has its required sections. */
    private static final String HEADER = """
            envelope:
              message_id: messageId
              schema_version: schemaVersion
              events_array: events
            event:
              event_id: eventId
            """;

    @SuppressWarnings("unchecked")
    private static MdpFieldMapping load(String yaml) {
        return MdpFieldMapping.fromYaml((Map<String, Object>) new Yaml().load(yaml));
    }

    @Test
    void parsesAllFourOperationTypes() {
        MdpFieldMapping m = load(HEADER + """
                derived:
                  - op: fallback
                    column: product_identity
                    sources: [instrumentId, accessoryId, subAssemblyId]
                  - op: array_reduce
                    column: spec_upper_limit
                    source_array: testSpecAttributes
                    match: UL
                    reduce: max
                  - op: numeric_string_split
                    source: measurementResult
                    numeric_column: measurement_result_numeric
                    string_column: measurement_result_text
                  - op: multi_format_timestamp
                    column: calibrated_at
                    source: calibratedAt
                    formats: [ISO_8601, "MM/dd/yyyy", "yyyy-MM-dd"]
                """);

        assertThat(m.operations).hasSize(4);
        assertThat(m.operations.get(0)).isEqualTo(
                new FallbackOp("product_identity", List.of("instrumentId", "accessoryId", "subAssemblyId")));
        // name_key/value_key omitted -> default to name/value.
        assertThat(m.operations.get(1)).isEqualTo(
                new ArrayReduceOp("spec_upper_limit", "testSpecAttributes", "name", "value", "UL", ReduceKind.MAX));
        assertThat(m.operations.get(2)).isEqualTo(
                new NumericStringSplitOp("measurementResult", "measurement_result_numeric", "measurement_result_text"));
        assertThat(m.operations.get(3)).isEqualTo(
                new MultiFormatTimestampOp("calibrated_at", "calibratedAt",
                        List.of("ISO_8601", "MM/dd/yyyy", "yyyy-MM-dd")));
    }

    @Test
    void mappingWithoutDerivedSection_isValidAndEmpty() {
        assertThat(load(HEADER).operations).isEmpty();
    }

    @Test
    void unknownOperation_failsFast() {
        assertThatThrownBy(() -> load(HEADER + """
                derived:
                  - op: not_a_real_op
                    column: x
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown derived op 'not_a_real_op'");
    }

    @Test
    void arrayReduceWithBadReduce_failsFast() {
        assertThatThrownBy(() -> load(HEADER + """
                derived:
                  - op: array_reduce
                    column: spec_upper_limit
                    source_array: testSpecAttributes
                    match: UL
                    reduce: average
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max or min");
    }

    @Test
    void splitWithSameNumericAndStringColumn_failsFast() {
        assertThatThrownBy(() -> load(HEADER + """
                derived:
                  - op: numeric_string_split
                    source: measurementResult
                    numeric_column: same
                    string_column: same
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must differ");
    }

    @Test
    void timestampWithInvalidPattern_failsFast() {
        assertThatThrownBy(() -> load(HEADER + """
                derived:
                  - op: multi_format_timestamp
                    column: calibrated_at
                    source: calibratedAt
                    formats: ["yyyy'MM"]
                """))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid date format");
    }
}
