package com.example.spaas.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.spaas.lineage.Hashing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * M1 walking skeleton: Kafka -> Flink -> Iceberg (REST catalog on MinIO) -> Trino.
 *
 * Reads {@code mdp.mfg.raw} and writes ONE near-passthrough row per Kafka MESSAGE
 * (raw payload + basic lineage) into {@code mdp.raw_events}. Flattening is trivial
 * here on purpose — this milestone just proves every pipe is connected with the
 * real vanilla connectors and the pinned version matrix. The config-driven
 * {@code FlattenerJob} (M2+) supersedes this for real per-event flattening.
 */
public final class RawEventsJob {

    // raw_events column order — MUST match scripts/sql/create_tables.sql exactly.
    static final String[] COLUMNS = {
            "message_id", "schema_version", "raw_payload",
            "source_topic", "source_partition", "source_offset", "kafka_timestamp",
            "raw_payload_hash", "processor_version", "processed_at"
    };

    static final RowType ROW_TYPE = RowType.of(
            new LogicalType[]{
                    new VarCharType(VarCharType.MAX_LENGTH), // message_id
                    new VarCharType(VarCharType.MAX_LENGTH), // schema_version
                    new VarCharType(VarCharType.MAX_LENGTH), // raw_payload
                    new VarCharType(VarCharType.MAX_LENGTH), // source_topic
                    new IntType(),                           // source_partition
                    new BigIntType(),                        // source_offset
                    new LocalZonedTimestampType(6),          // kafka_timestamp (timestamptz)
                    new VarCharType(VarCharType.MAX_LENGTH), // raw_payload_hash
                    new VarCharType(VarCharType.MAX_LENGTH), // processor_version
                    new LocalZonedTimestampType(6)           // processed_at (timestamptz)
            },
            COLUMNS);

    private RawEventsJob() {}

    public static void main(String[] args) throws Exception {
        final String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        final String topic = env("KAFKA_TOPIC", "mdp.mfg.raw");
        final String group = env("KAFKA_GROUP", "spaas-raw-events");
        final String table = env("TARGET_TABLE", "mdp.raw_events");
        final String processorVersion = env("PROCESSOR_VERSION", "0.1.0-m1");
        final long checkpointMs = Long.parseLong(env("CHECKPOINT_MS", "10000"));

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        // Iceberg's FlinkSink commits on checkpoint, so checkpointing MUST be on.
        senv.enableCheckpointing(checkpointMs);

        KafkaSource<RowData> source = KafkaSource.<RowData>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new RawRowDeserializer(processorVersion))
                .build();

        DataStream<RowData> rows =
                senv.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source-raw");

        CatalogLoader catalogLoader =
                CatalogLoader.rest("rest", new Configuration(false), IcebergConfig.fromEnv());
        TableLoader tableLoader =
                TableLoader.fromCatalog(catalogLoader, TableIdentifier.parse(table));

        FlinkSink.forRowData(rows)
                .tableLoader(tableLoader)
                .writeParallelism(1)
                .distributionMode(DistributionMode.NONE)
                .append();

        senv.execute("spaas-raw-events-m1");
    }

    static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    /**
     * Deserializes each Kafka record straight into an Iceberg-shaped {@link RowData},
     * capturing lineage (topic/partition/offset/timestamp + sha256 of raw bytes).
     * Best-effort extracts {@code messageId}/{@code schemaVersion}; unparseable payloads
     * still produce a row here (M1 is passthrough — quarantine arrives in M3).
     */
    static final class RawRowDeserializer implements KafkaRecordDeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final String processorVersion;
        private transient ObjectMapper mapper;

        RawRowDeserializer(String processorVersion) {
            this.processorVersion = processorVersion;
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> rec, Collector<RowData> out) {
            if (mapper == null) {
                mapper = new ObjectMapper();
            }
            byte[] value = rec.value() == null ? new byte[0] : rec.value();
            String raw = new String(value, StandardCharsets.UTF_8);

            String messageId = null;
            String schemaVersion = null;
            try {
                JsonNode n = mapper.readTree(value);
                if (n != null) {
                    messageId = text(n, "messageId");
                    schemaVersion = text(n, "schemaVersion");
                }
            } catch (Exception ignore) {
                // M1: keep passthrough rows even for unparseable payloads.
            }

            GenericRowData row = new GenericRowData(COLUMNS.length);
            row.setField(0, sd(messageId));
            row.setField(1, sd(schemaVersion));
            row.setField(2, sd(raw));
            row.setField(3, sd(rec.topic()));
            row.setField(4, rec.partition());
            row.setField(5, rec.offset());
            row.setField(6, TimestampData.fromEpochMillis(rec.timestamp()));
            row.setField(7, sd(Hashing.sha256Hex(value)));
            row.setField(8, sd(processorVersion));
            row.setField(9, TimestampData.fromInstant(Instant.now()));
            out.collect(row);
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(ROW_TYPE);
        }

        private static String text(JsonNode n, String field) {
            JsonNode f = n.get(field);
            return (f == null || f.isNull()) ? null : f.asText();
        }

        private static StringData sd(String s) {
            return s == null ? null : StringData.fromString(s);
        }
    }

    /** Iceberg REST + S3FileIO (MinIO) catalog properties from env, with local defaults. */
    static final class IcebergConfig {
        static Map<String, String> fromEnv() {
            Map<String, String> p = new HashMap<>();
            p.put("uri", env("ICEBERG_REST_URI", "http://iceberg-rest:8181"));
            p.put("warehouse", env("ICEBERG_WAREHOUSE", "s3://warehouse/"));
            p.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            p.put("s3.endpoint", env("S3_ENDPOINT", "http://minio:9000"));
            p.put("s3.path-style-access", "true");
            p.put("s3.access-key-id", env("AWS_ACCESS_KEY_ID", "admin"));
            p.put("s3.secret-access-key", env("AWS_SECRET_ACCESS_KEY", "password"));
            p.put("client.region", env("AWS_REGION", "us-east-1"));
            return p;
        }
    }
}
