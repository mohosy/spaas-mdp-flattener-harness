package com.example.spaas.api;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Outcome of transforming ONE raw message: zero or more flattened output rows,
 * zero or more quarantined records, plus counters for the audit output.
 *
 * Each flattened row is a column-name -&gt; value map (business + lineage columns);
 * the job converts it to Iceberg {@code RowData} via {@code FlattenedSchema}.
 */
public final class TransformResult implements Serializable {
    private static final long serialVersionUID = 1L;

    public final List<Map<String, Object>> flattenedRows;
    public final List<QuarantineRecord> quarantined;
    /** Number of events seen in the message (before dedup), for audit input_event_count. */
    public final int inputEventCount;
    /** Number of events dropped by intra-message dedup, for audit dedup_dropped_count. */
    public final int dedupDropped;

    public TransformResult(List<Map<String, Object>> flattenedRows,
                           List<QuarantineRecord> quarantined,
                           int inputEventCount,
                           int dedupDropped) {
        this.flattenedRows = flattenedRows;
        this.quarantined = quarantined;
        this.inputEventCount = inputEventCount;
        this.dedupDropped = dedupDropped;
    }

    public static TransformResult quarantineOnly(QuarantineRecord q, int inputEventCount) {
        List<QuarantineRecord> qs = new ArrayList<>(1);
        qs.add(q);
        return new TransformResult(new ArrayList<>(), qs, inputEventCount, 0);
    }

    public int outputRowCount() {
        return flattenedRows.size();
    }
}
