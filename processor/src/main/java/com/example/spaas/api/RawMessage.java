package com.example.spaas.api;

import java.io.Serializable;

/**
 * One raw Kafka message plus its source lineage, as it enters the pipeline.
 *
 * A Flink POJO (public no-arg ctor + public fields) so Flink uses its efficient
 * POJO serializer rather than Kryo when this flows between operators.
 */
public class RawMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public byte[] value;
    public String topic;
    public int partition;
    public long offset;
    public long timestampMs;

    public RawMessage() {}

    public RawMessage(byte[] value, String topic, int partition, long offset, long timestampMs) {
        this.value = value;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestampMs = timestampMs;
    }
}
