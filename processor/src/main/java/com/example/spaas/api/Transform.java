package com.example.spaas.api;

import java.io.Serializable;

/**
 * Generic transform contract: convert one raw Kafka message into zero or more
 * output rows, routing bad records to quarantine.
 *
 * Contract:
 * <ul>
 *   <li><b>Deterministic</b> — identical input always yields identical output
 *       (including the {@code canonical_row_hash}).</li>
 *   <li><b>Never throws</b> on bad input — malformed/invalid records become
 *       {@link QuarantineRecord}s, never exceptions that crash the pipeline.</li>
 * </ul>
 *
 * The MDP flattener is the first implementation; a new transform is a new class
 * plus config — no pipeline rewiring. See {@code com.example.spaas.transform.mdp.MdpFlattener}.
 */
public interface Transform extends Serializable {

    TransformResult apply(RawMessage message);

    /** Stable name for logging/audit (e.g. "mdp-flattener"). */
    String name();
}
