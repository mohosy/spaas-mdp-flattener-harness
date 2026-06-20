package com.example.spaas.config;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * The single, validated job configuration (config/job.yaml). Selects source,
 * transform, sinks (flattened/quarantine/audit), Iceberg target and runtime
 * metadata. {@link #load(String)} validates required keys on startup and fails
 * fast with a clear message if anything is missing.
 */
public final class JobConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // processor
    public final String processorVersion;
    // source
    public final String bootstrapServers;
    public final String topic;
    public final String groupId;
    public final String startupMode;
    // transform
    public final String transformType;
    public final String mappingFile;
    // iceberg
    public final String restUri;
    public final String warehouse;
    public final String s3Endpoint;
    public final String region;
    // sinks
    public final String flattenedTable;
    public final String quarantineTable;
    public final String auditTable;
    // runtime
    public final long checkpointMs;
    public final int parallelism;

    private JobConfig(String processorVersion, String bootstrapServers, String topic, String groupId,
                      String startupMode, String transformType, String mappingFile, String restUri,
                      String warehouse, String s3Endpoint, String region, String flattenedTable,
                      String quarantineTable, String auditTable, long checkpointMs, int parallelism) {
        this.processorVersion = processorVersion;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.startupMode = startupMode;
        this.transformType = transformType;
        this.mappingFile = mappingFile;
        this.restUri = restUri;
        this.warehouse = warehouse;
        this.s3Endpoint = s3Endpoint;
        this.region = region;
        this.flattenedTable = flattenedTable;
        this.quarantineTable = quarantineTable;
        this.auditTable = auditTable;
        this.checkpointMs = checkpointMs;
        this.parallelism = parallelism;
    }

    /** Programmatic config for tests (bypasses YAML loading; still validated). */
    public static JobConfig forTest(String bootstrap, String topic, String groupId, String restUri,
                                    String warehouse, String s3Endpoint, String region,
                                    String flattened, String quarantine, String audit,
                                    long checkpointMs, String processorVersion) {
        JobConfig c = new JobConfig(processorVersion, bootstrap, topic, groupId, "earliest",
                "mdp_flatten", "(programmatic)", restUri, warehouse, s3Endpoint, region,
                flattened, quarantine, audit, checkpointMs, 1);
        c.validate();
        return c;
    }

    @SuppressWarnings("unchecked")
    public static JobConfig load(String path) {
        Map<String, Object> root;
        try (InputStream in = new FileInputStream(path)) {
            Object loaded = new Yaml().load(in);
            if (!(loaded instanceof Map)) {
                throw new IllegalArgumentException("job config did not parse to a map: " + path);
            }
            root = (Map<String, Object>) loaded;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read job config: " + path, e);
        }

        Map<String, Object> processor = section(root, "processor");
        Map<String, Object> source = section(root, "source");
        Map<String, Object> transform = section(root, "transform");
        Map<String, Object> iceberg = section(root, "iceberg");
        Map<String, Object> sinks = section(root, "sinks");
        Map<String, Object> runtime = section(root, "runtime");

        JobConfig cfg = new JobConfig(
                req(processor, "version", "processor"),
                req(source, "bootstrap_servers", "source"),
                req(source, "topic", "source"),
                req(source, "group_id", "source"),
                opt(source, "startup_mode", "earliest"),
                req(transform, "type", "transform"),
                req(transform, "mapping_file", "transform"),
                req(iceberg, "rest_uri", "iceberg"),
                req(iceberg, "warehouse", "iceberg"),
                req(iceberg, "s3_endpoint", "iceberg"),
                opt(iceberg, "region", "us-east-1"),
                req(sinks, "flattened", "sinks"),
                req(sinks, "quarantine", "sinks"),
                req(sinks, "audit", "sinks"),
                Long.parseLong(opt(runtime, "checkpoint_ms", "10000")),
                Integer.parseInt(opt(runtime, "parallelism", "1")));
        cfg.validate();
        return cfg;
    }

    private void validate() {
        if (parallelism != 1) {
            // Hard constraint: single writer per Iceberg table.
            throw new IllegalArgumentException(
                    "runtime.parallelism must be 1 (single writer per Iceberg table); got " + parallelism);
        }
        if (!"earliest".equals(startupMode) && !"latest".equals(startupMode)) {
            throw new IllegalArgumentException("source.startup_mode must be 'earliest' or 'latest'");
        }
        if (checkpointMs <= 0) {
            throw new IllegalArgumentException("runtime.checkpoint_ms must be > 0");
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> section(Map<String, Object> root, String name) {
        Object s = root.get(name);
        if (!(s instanceof Map)) {
            throw new IllegalArgumentException("job config missing required section '" + name + "'");
        }
        return (Map<String, Object>) s;
    }

    private static String req(Map<String, Object> m, String key, String section) {
        Object v = m.get(key);
        if (v == null || String.valueOf(v).isBlank()) {
            throw new IllegalArgumentException("job config missing required key '" + section + "." + key + "'");
        }
        return String.valueOf(v);
    }

    private static String opt(Map<String, Object> m, String key, String def) {
        Object v = m.get(key);
        return (v == null || String.valueOf(v).isBlank()) ? def : String.valueOf(v);
    }
}
