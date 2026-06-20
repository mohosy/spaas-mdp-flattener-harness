package com.example.spaas.api;

import java.io.Serializable;

/**
 * A rejected record routed to the quarantine sink. Produced when a message cannot
 * be parsed as JSON or an event is missing a required field. The pipeline must
 * NEVER crash on a bad record — it produces one of these instead.
 */
public class QuarantineRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String rawPayload;
    public final String errorReason;   // short machine-ish reason, e.g. MALFORMED_JSON
    public final String errorDetail;   // human detail (exception msg / which field)
    public final String topic;
    public final int partition;
    public final long offset;
    public final long timestampMs;

    public QuarantineRecord(String rawPayload, String errorReason, String errorDetail,
                            String topic, int partition, long offset, long timestampMs) {
        this.rawPayload = rawPayload;
        this.errorReason = errorReason;
        this.errorDetail = errorDetail;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestampMs = timestampMs;
    }

    /** Reason codes (stable strings, queryable in the quarantine table). */
    public static final class Reason {
        public static final String MALFORMED_JSON = "MALFORMED_JSON";
        public static final String NOT_AN_ENVELOPE = "NOT_AN_ENVELOPE";
        public static final String MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD";
        public static final String INVALID_FIELD_VALUE = "INVALID_FIELD_VALUE";
        private Reason() {}
    }
}
