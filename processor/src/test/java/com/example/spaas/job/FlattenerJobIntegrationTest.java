package com.example.spaas.job;

import com.example.spaas.api.AuditSchema;
import com.example.spaas.api.FlattenedSchema;
import com.example.spaas.api.QuarantineSchema;
import com.example.spaas.config.JobConfig;
import com.example.spaas.transform.mdp.MdpFieldMapping;

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Type;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test on ephemeral containers (Kafka + MinIO + Iceberg REST).
 * Runs the EXACT production pipeline ({@link FlattenerJob#buildPipeline}) on a Flink
 * MiniCluster and asserts: good rows land in flattened_measurements, bad rows land in
 * quarantine, and audit is written. Tagged 'integration' — run with `make test integration`.
 */
@Tag("integration")
class FlattenerJobIntegrationTest {

    private static final Network NET = Network.newNetwork();

    private static final MinIOContainer MINIO = new MinIOContainer(
            DockerImageName.parse("minio/minio:RELEASE.2025-09-07T16-13-09Z"))
            .withUserName("admin").withPassword("password")
            .withNetwork(NET).withNetworkAliases("minio");

    private static final GenericContainer<?> REST = new GenericContainer<>(
            DockerImageName.parse("apache/iceberg-rest-fixture:1.10.1"))
            .withNetwork(NET).withNetworkAliases("iceberg-rest")
            .withExposedPorts(8181)
            .withEnv("CATALOG_WAREHOUSE", "s3://warehouse/")
            .withEnv("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .withEnv("CATALOG_S3_ENDPOINT", "http://minio:9000")
            .withEnv("CATALOG_S3_PATH__STYLE__ACCESS", "true")
            .withEnv("CATALOG_S3_ACCESS__KEY__ID", "admin")
            .withEnv("CATALOG_S3_SECRET__ACCESS__KEY", "password")
            .withEnv("AWS_ACCESS_KEY_ID", "admin")
            .withEnv("AWS_SECRET_ACCESS_KEY", "password")
            .withEnv("AWS_REGION", "us-east-1")
            .waitingFor(Wait.forHttp("/v1/config").forStatusCode(200));

    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.2"));

    private static String restUri;
    private static String minioUrl;
    private static String bootstrap;
    private static final String TOPIC = "mdp.mfg.raw";

    @BeforeAll
    static void up() throws Exception {
        MINIO.start();
        minioUrl = MINIO.getS3URL();
        createBucket(minioUrl, "warehouse");
        REST.start();
        KAFKA.start();
        restUri = "http://" + REST.getHost() + ":" + REST.getMappedPort(8181);
        bootstrap = KAFKA.getBootstrapServers();
        createTopic(bootstrap, TOPIC);
        createTables();
    }

    @AfterAll
    static void down() {
        KAFKA.stop();
        REST.stop();
        MINIO.stop();
    }

    @Test
    @Timeout(240)
    void endToEnd_goodRowsLand_badRowsQuarantine_auditWritten() throws Exception {
        produceMessages();

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.enableCheckpointing(2000);   // small -> audit timer fires quickly

        JobConfig cfg = JobConfig.forTest(bootstrap, TOPIC, "it-flattener", restUri,
                "s3://warehouse/", minioUrl, "us-east-1",
                "mdp.flattened_measurements", "mdp.quarantine", "mdp.audit",
                2000, "it-0.0");
        FlattenerJob.buildPipeline(senv, cfg, MdpFieldMapping.defaultSynthetic());
        JobClient client = senv.executeAsync("it-flattener");

        RESTCatalog catalog = catalog();
        long flat = 0, quar = 0, aud = 0;
        long deadline = System.currentTimeMillis() + 90_000;
        while (System.currentTimeMillis() < deadline) {
            flat = totalRecords(catalog, "mdp.flattened_measurements");
            quar = totalRecords(catalog, "mdp.quarantine");
            aud = totalRecords(catalog, "mdp.audit");
            if (flat >= 7 && quar >= 2 && aud >= 1) {
                break;
            }
            Thread.sleep(3000);
        }
        client.cancel();
        catalog.close();
        System.out.println("[IT] FINAL flattened=" + flat + " quarantine=" + quar + " audit=" + aud);

        // 3 good msgs x2 events + 1 dedup msg (2 identical -> 1) = 7 flattened rows.
        assertThat(flat).as("flattened good rows").isEqualTo(7);
        // 1 malformed-json + 1 missing-required-field = 2 quarantined.
        assertThat(quar).as("quarantined bad rows").isEqualTo(2);
        assertThat(aud).as("audit rows written").isGreaterThanOrEqualTo(1);
    }

    // ----------------------------------------------------------------- helpers

    private static void createBucket(String endpoint, String bucket) {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("admin", "password")))
                .build()) {
            s3.createBucket(b -> b.bucket(bucket));
        }
    }

    private static void createTopic(String bootstrap, String topic) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(p)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private static RESTCatalog catalog() {
        RESTCatalog c = new RESTCatalog();
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.URI, restUri);
        props.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://warehouse/");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put("s3.endpoint", minioUrl);
        props.put("s3.path-style-access", "true");
        props.put("s3.access-key-id", "admin");
        props.put("s3.secret-access-key", "password");
        props.put("client.region", "us-east-1");
        c.initialize("rest", props);
        return c;
    }

    private static void createTables() throws Exception {
        RESTCatalog c = catalog();
        c.createNamespace(Namespace.of("mdp"));
        c.createTable(TableIdentifier.of("mdp", "flattened_measurements"), toSchema(FlattenedSchema.ROW_TYPE));
        c.createTable(TableIdentifier.of("mdp", "quarantine"), toSchema(QuarantineSchema.ROW_TYPE));
        c.createTable(TableIdentifier.of("mdp", "audit"), toSchema(AuditSchema.ROW_TYPE));
        c.close();
    }

    /** RowType -> Iceberg Schema (convert(RowType) yields a Type; wrap its struct fields). */
    private static Schema toSchema(RowType rowType) {
        Type type = FlinkSchemaUtil.convert(rowType);
        return new Schema(type.asStructType().fields());
    }

    private static long totalRecords(RESTCatalog catalog, String name) {
        Table t = catalog.loadTable(TableIdentifier.parse(name));
        t.refresh();
        Snapshot s = t.currentSnapshot();
        return s == null ? 0 : Long.parseLong(s.summary().getOrDefault("total-records", "0"));
    }

    private static void produceMessages() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", StringSerializer.class.getName());
        p.put("value.serializer", ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(p)) {
            // 3 good messages, 2 distinct events each -> 6 good rows
            send(producer, envelope("m1", event("e1", "SN-1", 12.0), event("e2", "SN-2", 11.0)));
            send(producer, envelope("m2", event("e3", "SN-3", 10.5), event("e4", "SN-4", 13.0)));
            send(producer, envelope("m3", event("e5", "SN-5", 9.5), event("e6", "SN-6", 14.0)));
            // 1 message with two IDENTICAL events -> 1 row + 1 dedup-dropped
            String dup = event("e7", "SN-7", 12.34);
            send(producer, envelope("m4", dup, dup));
            // 1 malformed (non-JSON) -> quarantine
            send(producer, "{ this is not valid json %%%".getBytes(StandardCharsets.UTF_8));
            // 1 event missing required productSerial -> quarantine
            String bad = "{\"eventId\":\"e9\",\"measurementName\":\"x\",\"measurementValue\":1.0,"
                    + "\"measuredAt\":\"2026-06-19T10:00:00.050Z\"}";
            send(producer, envelope("m5", bad));
            producer.flush();
        }
    }

    private static void send(KafkaProducer<String, byte[]> producer, byte[] value) {
        producer.send(new ProducerRecord<>(TOPIC, "k", value));
    }

    private static void send(KafkaProducer<String, byte[]> producer, String json) {
        send(producer, json.getBytes(StandardCharsets.UTF_8));
    }

    private static String event(String eventId, String serial, double value) {
        return "{\"eventId\":\"" + eventId + "\",\"testStationId\":\"EFT-07\","
                + "\"productModel\":\"RX-5\",\"productSerial\":\"" + serial + "\","
                + "\"testProgram\":\"FF\",\"testSessionId\":\"s1\",\"operatorId\":\"op-1\","
                + "\"measurementName\":\"torque\",\"measurementValue\":" + value + ","
                + "\"measurementUnit\":\"Nm\",\"lowerLimit\":10.0,\"upperLimit\":15.0,"
                + "\"result\":\"PASS\",\"measuredAt\":\"2026-06-19T10:00:00.050Z\"}";
    }

    private static String envelope(String messageId, String... events) {
        return "{\"messageId\":\"" + messageId + "\",\"schemaVersion\":\"mdp.mfg.v1\","
                + "\"producedAt\":\"2026-06-19T10:00:00.123Z\",\"events\":[" + String.join(",", events) + "]}";
    }
}
