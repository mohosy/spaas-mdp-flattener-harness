package com.example.spaas.job;

import com.example.spaas.api.AuditSchema;
import com.example.spaas.api.FlattenedSchema;
import com.example.spaas.api.QuarantineRecord;
import com.example.spaas.api.QuarantineSchema;
import com.example.spaas.api.RawMessage;
import com.example.spaas.api.TransformResult;
import com.example.spaas.config.JobConfig;
import com.example.spaas.transform.mdp.MdpFieldMapping;
import com.example.spaas.transform.mdp.MdpFlattener;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Config-driven MDP flattener job (M2 flattening + M3 quarantine/audit).
 *
 * Pipeline: Kafka -&gt; keyBy(constant) -&gt; FlattenProcess -&gt; three Iceberg sinks:
 *   main output      -&gt; mdp.flattened_measurements
 *   QUARANTINE side  -&gt; mdp.quarantine
 *   AUDIT side       -&gt; mdp.audit  (one row per audit window via processing-time timer)
 *
 * Everything is selected by config/job.yaml + field-mapping.yaml. Single writer per
 * table (parallelism 1) is enforced by JobConfig.
 */
public final class FlattenerJob {

    static final OutputTag<RowData> QUARANTINE_TAG =
            new OutputTag<>("quarantine-output", InternalTypeInfo.of(QuarantineSchema.ROW_TYPE));
    static final OutputTag<RowData> AUDIT_TAG =
            new OutputTag<>("audit-output", InternalTypeInfo.of(AuditSchema.ROW_TYPE));

    private FlattenerJob() {}

    public static void main(String[] args) throws Exception {
        String configPath = System.getenv().getOrDefault("CONFIG_PATH", "/config/job.yaml");
        JobConfig cfg = JobConfig.load(configPath);
        MdpFieldMapping mapping = MdpFieldMapping.fromYamlFile(cfg.mappingFile);
        Map<String, String> catalogProps = icebergProps(cfg);

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(cfg.parallelism);
        senv.enableCheckpointing(cfg.checkpointMs);

        buildPipeline(senv, cfg, mapping);

        senv.execute("spaas-mdp-flattener");
    }

    /**
     * Wires Kafka source -&gt; flatten -&gt; three Iceberg sinks onto the given environment.
     * Extracted so the integration test can drive the exact production pipeline on a
     * Flink MiniCluster. The caller owns env-level config (parallelism, checkpointing).
     */
    public static void buildPipeline(StreamExecutionEnvironment senv, JobConfig cfg,
                                     MdpFieldMapping mapping) {
        Map<String, String> catalogProps = icebergProps(cfg);

        OffsetsInitializer start = "latest".equals(cfg.startupMode)
                ? OffsetsInitializer.latest() : OffsetsInitializer.earliest();

        KafkaSource<RawMessage> source = KafkaSource.<RawMessage>builder()
                .setBootstrapServers(cfg.bootstrapServers)
                .setTopics(cfg.topic)
                .setGroupId(cfg.groupId)
                .setStartingOffsets(start)
                .setDeserializer(new RawMessageDeserializer())
                .build();

        DataStream<RawMessage> src =
                senv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");

        SingleOutputStreamOperator<RowData> flattened = src
                .keyBy((KeySelector<RawMessage, Integer>) m -> 0)
                .process(new FlattenProcess(mapping, cfg.processorVersion, cfg.topic,
                        cfg.checkpointMs, catalogProps, cfg.flattenedTable))
                .name("mdp-flatten")
                .returns(InternalTypeInfo.of(FlattenedSchema.ROW_TYPE));

        DataStream<RowData> quarantine = flattened.getSideOutput(QUARANTINE_TAG);
        DataStream<RowData> audit = flattened.getSideOutput(AUDIT_TAG);

        CatalogLoader catalogLoader = CatalogLoader.rest(
                "rest", new org.apache.hadoop.conf.Configuration(false), catalogProps);

        sinkTo(flattened, catalogLoader, cfg.flattenedTable, "flattened-sink");
        sinkTo(quarantine, catalogLoader, cfg.quarantineTable, "quarantine-sink");
        sinkTo(audit, catalogLoader, cfg.auditTable, "audit-sink");
    }

    private static void sinkTo(DataStream<RowData> rows, CatalogLoader cl, String table, String name) {
        TableLoader tl = TableLoader.fromCatalog(cl, TableIdentifier.parse(table));
        FlinkSink.forRowData(rows)
                .tableLoader(tl)
                .writeParallelism(1)
                .distributionMode(DistributionMode.NONE)
                .append()
                .name(name);
    }

    static Map<String, String> icebergProps(JobConfig cfg) {
        Map<String, String> p = new HashMap<>();
        p.put("uri", cfg.restUri);
        p.put("warehouse", cfg.warehouse);
        p.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        p.put("s3.endpoint", cfg.s3Endpoint);
        p.put("s3.path-style-access", "true");
        p.put("s3.access-key-id", env("AWS_ACCESS_KEY_ID", "admin"));
        p.put("s3.secret-access-key", env("AWS_SECRET_ACCESS_KEY", "password"));
        p.put("client.region", cfg.region);
        return p;
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    /** Emits one RawMessage per Kafka record with source lineage. */
    static final class RawMessageDeserializer implements KafkaRecordDeserializationSchema<RawMessage> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<RawMessage> out) {
            out.collect(new RawMessage(rec.value(), rec.topic(), rec.partition(), rec.offset(), rec.timestamp()));
        }

        @Override
        public TypeInformation<RawMessage> getProducedType() {
            return TypeInformation.of(RawMessage.class);
        }
    }

    /**
     * Flattens each message, routes quarantine to a side output, and emits a periodic
     * audit record (one per checkpoint-interval window) to the audit side output.
     *
     * Counters are per-window instance state (reset each audit emit). They are NOT in
     * Flink keyed state, so on restart auditing simply begins a fresh window — the
     * flattened data itself is exactly-/at-least-once via the Iceberg sink + checkpoints.
     */
    static final class FlattenProcess
            extends KeyedProcessFunction<Integer, RawMessage, RowData> {
        private static final long serialVersionUID = 1L;

        private final MdpFieldMapping mapping;
        private final String processorVersion;
        private final String topic;
        private final long auditIntervalMs;
        private final Map<String, String> catalogProps;
        private final String flattenedTable;

        private transient MdpFlattener flattener;
        private transient String runId;
        private transient boolean timerArmed;
        private transient long inputEvents;
        private transient long outputRows;
        private transient long quarantineCnt;
        private transient long dedupDropped;
        private transient TreeMap<Integer, long[]> partOffsets; // partition -> [min,max]
        private transient Catalog catalog;

        FlattenProcess(MdpFieldMapping mapping, String processorVersion, String topic,
                       long auditIntervalMs, Map<String, String> catalogProps, String flattenedTable) {
            this.mapping = mapping;
            this.processorVersion = processorVersion;
            this.topic = topic;
            this.auditIntervalMs = auditIntervalMs;
            this.catalogProps = catalogProps;
            this.flattenedTable = flattenedTable;
        }

        private void lazyInit() {
            if (flattener == null) {
                flattener = new MdpFlattener(mapping, processorVersion);
                runId = UUID.randomUUID().toString();
                partOffsets = new TreeMap<>();
            }
        }

        @Override
        public void processElement(RawMessage msg, Context ctx, Collector<RowData> out) {
            lazyInit();
            if (!timerArmed) {
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + auditIntervalMs);
                timerArmed = true;
            }

            TransformResult r = flattener.apply(msg);
            for (Map<String, Object> row : r.flattenedRows) {
                out.collect(FlattenedSchema.toRowData(row));
                outputRows++;
            }
            for (QuarantineRecord qr : r.quarantined) {
                ctx.output(QUARANTINE_TAG, QuarantineSchema.toRowData(qr, Instant.now()));
                quarantineCnt++;
            }
            inputEvents += r.inputEventCount;
            dedupDropped += r.dedupDropped;

            long[] mm = partOffsets.computeIfAbsent(msg.partition, k -> new long[]{msg.offset, msg.offset});
            if (msg.offset < mm[0]) mm[0] = msg.offset;
            if (msg.offset > mm[1]) mm[1] = msg.offset;
        }

        @Override
        public void onTimer(long ts, OnTimerContext ctx, Collector<RowData> out) {
            if (inputEvents > 0 || quarantineCnt > 0) {
                ctx.output(AUDIT_TAG, AuditSchema.toRowData(
                        runId, processorVersion, topic, partitionOffsetsJson(),
                        inputEvents, outputRows, quarantineCnt, dedupDropped,
                        Instant.now(), trySnapshotId()));
                inputEvents = 0;
                outputRows = 0;
                quarantineCnt = 0;
                dedupDropped = 0;
                partOffsets.clear();
            }
            ctx.timerService().registerProcessingTimeTimer(ts + auditIntervalMs);
        }

        private String partitionOffsetsJson() {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<Integer, long[]> e : partOffsets.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(e.getKey()).append("\":{\"min\":")
                        .append(e.getValue()[0]).append(",\"max\":").append(e.getValue()[1]).append('}');
            }
            return sb.append('}').toString();
        }

        /** Best-effort: current snapshot id of the flattened table, or null. */
        private Long trySnapshotId() {
            try {
                if (catalog == null) {
                    catalog = CatalogLoader.rest(
                            "rest", new org.apache.hadoop.conf.Configuration(false), catalogProps).loadCatalog();
                }
                Table t = catalog.loadTable(TableIdentifier.parse(flattenedTable));
                Snapshot s = t.currentSnapshot();
                return s == null ? null : s.snapshotId();
            } catch (Exception e) {
                return null;
            }
        }
    }
}
