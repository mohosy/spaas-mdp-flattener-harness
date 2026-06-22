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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Config-driven MDP flattener job (M2 flattening + M3 quarantine/audit).
 *
 * Pipeline: Kafka -&gt; keyBy(constant) -&gt; FlattenProcess -&gt; three Iceberg sinks:
 *   main output      -&gt; mdp.flattened_measurements
 *   QUARANTINE side  -&gt; mdp.quarantine
 *   AUDIT side       -&gt; mdp.audit  (one row per Iceberg commit, flushed by a timer)
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
     * Flattens each message, routes quarantine to a side output, and emits one audit row
     * per Iceberg commit to the audit side output.
     *
     * The audit window is designed around two Flink rules: a function may not emit records
     * from snapshotState or notifyCheckpointComplete, and a bounded input (produced once,
     * then idle) must still flush an audit row. The mechanism, in four steps:
     * <ol>
     *   <li>processElement accumulates the running counters into transient working fields.</li>
     *   <li>snapshotState seals those counters into a pending audit window tagged with this
     *       checkpoint id, then resets the running fields. The window holds exactly the
     *       records the Iceberg sink commits for this checkpoint. Sealed windows are written
     *       to operator managed state (a ListState), so the counters survive a checkpoint and
     *       a restart. The running fields are transient on purpose: a restart rewinds the
     *       Kafka source and reprocesses the records after the last checkpoint, so persisting
     *       a partial window would double count it. This is the standard operator state
     *       pattern (a working copy synced to managed state at each checkpoint).</li>
     *   <li>notifyCheckpointComplete records the highest completed checkpoint id. The Iceberg
     *       commit for that checkpoint has now landed, so its snapshot id is readable.</li>
     *   <li>a processing time timer (armed by the first element, re-registered on every fire)
     *       emits each pending window whose checkpoint has completed, stamped with the
     *       committed snapshot id. Emitting from the timer rather than from the next element
     *       is what keeps a bounded run from starving: the timer keeps firing on wall clock
     *       even when no new element arrives, so the last sealed window is still written.</li>
     * </ol>
     * Parallelism is fixed at 1 (single writer per table), so the constant keyBy and the
     * operator state both describe one logical writer; the constant key exists only so this
     * function can register the flush timer. Flattened data is exactly once via the Iceberg
     * sink; audit rows are at least once (a window may repeat if the job restarts before the
     * next checkpoint trims the emitted entry from managed state).
     */
    static final class FlattenProcess
            extends KeyedProcessFunction<Integer, RawMessage, RowData>
            implements CheckpointedFunction, CheckpointListener {
        private static final long serialVersionUID = 1L;

        private final MdpFieldMapping mapping;
        private final String processorVersion;
        private final String topic;
        private final long flushIntervalMs;
        private final Map<String, String> catalogProps;
        private final String flattenedTable;

        private transient MdpFlattener flattener;
        private transient String runId;
        private transient boolean timerArmed;
        // Running window: counters accumulated since the last checkpoint. Working copy that
        // is sealed into managed state at each checkpoint (see class Javadoc).
        private transient long inputEvents;
        private transient long outputRows;
        private transient long quarantineCnt;
        private transient long dedupDropped;
        private transient TreeMap<Integer, long[]> partOffsets; // partition -> [min,max]
        // Sealed windows awaiting emit. The in-memory list is the working copy; pendingState
        // is the managed state it is synced to at each checkpoint and restored from on start.
        private transient List<AuditWindow> pending;
        private transient ListState<AuditWindow> pendingState;
        private transient long lastCompletedCheckpointId;
        private transient Catalog catalog;

        FlattenProcess(MdpFieldMapping mapping, String processorVersion, String topic,
                       long flushIntervalMs, Map<String, String> catalogProps, String flattenedTable) {
            this.mapping = mapping;
            this.processorVersion = processorVersion;
            this.topic = topic;
            this.flushIntervalMs = flushIntervalMs;
            this.catalogProps = catalogProps;
            this.flattenedTable = flattenedTable;
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            flattener = new MdpFlattener(mapping, processorVersion);
            runId = UUID.randomUUID().toString();
            partOffsets = new TreeMap<>();
            pendingState = ctx.getOperatorStateStore().getListState(
                    new ListStateDescriptor<>("audit-pending", TypeInformation.of(AuditWindow.class)));
            pending = new ArrayList<>();
            for (AuditWindow w : pendingState.get()) {   // restore windows sealed before a restart
                pending.add(w);
            }
        }

        @Override
        public void processElement(RawMessage msg, Context ctx, Collector<RowData> out) {
            if (!timerArmed) {
                ctx.timerService().registerProcessingTimeTimer(
                        ctx.timerService().currentProcessingTime() + flushIntervalMs);
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
            emitCompletedWindows(ctx);
            ctx.timerService().registerProcessingTimeTimer(ts + flushIntervalMs);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
            // Seal the records gathered since the last checkpoint into one window tagged with
            // this checkpoint id; the Iceberg sink commits these same records when this
            // checkpoint completes. Empty windows are skipped so audit has no blank rows.
            if (inputEvents > 0 || quarantineCnt > 0) {
                pending.add(new AuditWindow(runId, inputEvents, outputRows, quarantineCnt,
                        dedupDropped, partitionOffsetsJson(), ctx.getCheckpointId()));
                resetRunningWindow();
            }
            pendingState.update(pending);   // single point where the pending list is persisted
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            // The Iceberg commit for this checkpoint has landed, so its snapshot id is now
            // readable. The flush timer emits pending windows up to this checkpoint id.
            lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointId);
        }

        /** Emits each pending window whose checkpoint has committed, with that snapshot id. */
        private void emitCompletedWindows(OnTimerContext ctx) {
            if (pending.isEmpty()) {
                return;
            }
            Long snapshotId = trySnapshotId();   // committed snapshot of the flattened table
            Iterator<AuditWindow> it = pending.iterator();
            while (it.hasNext()) {
                AuditWindow w = it.next();
                if (w.checkpointId > lastCompletedCheckpointId) {
                    continue;   // its Iceberg commit has not completed yet; a later timer emits it
                }
                ctx.output(AUDIT_TAG, AuditSchema.toRowData(
                        w.runId, processorVersion, topic, w.partitionOffsetsJson,
                        w.inputEvents, w.outputRows, w.quarantineCnt, w.dedupDropped,
                        Instant.now(), snapshotId));
                it.remove();
            }
        }

        private void resetRunningWindow() {
            inputEvents = 0;
            outputRows = 0;
            quarantineCnt = 0;
            dedupDropped = 0;
            partOffsets = new TreeMap<>();
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

    /**
     * One sealed audit window awaiting emit: the counters gathered for a single checkpoint,
     * the per partition offset range as JSON, and the checkpoint id that sealed it. A Flink
     * POJO (public no arg constructor, public fields) so operator state stores it with the
     * POJO serializer rather than with Kryo.
     */
    public static final class AuditWindow {
        public String runId;
        public long inputEvents;
        public long outputRows;
        public long quarantineCnt;
        public long dedupDropped;
        public String partitionOffsetsJson;
        public long checkpointId;

        public AuditWindow() {}

        AuditWindow(String runId, long inputEvents, long outputRows, long quarantineCnt,
                    long dedupDropped, String partitionOffsetsJson, long checkpointId) {
            this.runId = runId;
            this.inputEvents = inputEvents;
            this.outputRows = outputRows;
            this.quarantineCnt = quarantineCnt;
            this.dedupDropped = dedupDropped;
            this.partitionOffsetsJson = partitionOffsetsJson;
            this.checkpointId = checkpointId;
        }
    }
}
