// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry;

import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.*;

public class JdbcSourceBuilderTool {

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadAsNestedMap(InputStream inputStream) {
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        Object loaded = yaml.load(inputStream);
        if (loaded == null) {
            return new HashMap<>();
        }
        return (Map<String, Object>) loaded;
    }

    private Map<String, Object> loadAsFlatMap(InputStream inputStream) {
        Map<String, Object> nestedMap = loadAsNestedMap(inputStream);
        Map<String, Object> flatMap = new HashMap<>();
        flatten("", nestedMap, flatMap);
        return flatMap;
    }

    @SuppressWarnings("unchecked")
    private void flatten(
            String parentKey,
            Map<String, Object> source,
            Map<String, Object> target) {

        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String key = parentKey.isEmpty()
                    ? entry.getKey()
                    : parentKey + "." + entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Map) {
                flatten(key, (Map<String, Object>) value, target);
            } else {
                target.put(key, value);
            }
        }
    }

    public PostgresSourceBuilder<BinarySourceRecord> buildPostgresSource(
            String cdcConfigYamlPath,
            PostgresSourceBuilder<BinarySourceRecord> sourceBuilder)
            throws IOException {

        Path path = new Path(cdcConfigYamlPath);
        FileSystem fs = FileSystem.get(path.toUri());

        Map<String, Object> cdcParams;
        try (InputStream input = fs.open(path)) {
            cdcParams = loadAsFlatMap(input);
        }

        /* ------------------ Debezium ------------------ */
        Properties debeziumProperties = getDebeziumProperties(cdcParams);
        if (!debeziumProperties.isEmpty()) {
            sourceBuilder.debeziumProperties(debeziumProperties);
        }

        /* ------------------ fetch / split ------------------ */
        if (cdcParams.containsKey("scan.snapshot.fetch.size")) {
            sourceBuilder.fetchSize(
                    Integer.parseInt(cdcParams.get("scan.snapshot.fetch.size").toString()));
        }

        if (cdcParams.containsKey("chunk-meta.group.size")) {
            sourceBuilder.splitMetaGroupSize(
                    Integer.parseInt(cdcParams.get("chunk-meta.group.size").toString()));
        }

        /* ------------------ startup ------------------ */
        if (cdcParams.containsKey("scan.startup.mode")) {
            sourceBuilder.startupOptions(
                    parsePgStartupOptions(
                            cdcParams.get("scan.startup.mode").toString()));
        }

        /* ------------------ heartbeat ------------------ */
        if (cdcParams.containsKey("heartbeat.interval")) {
            sourceBuilder.heartbeatInterval(
                    Duration.ofSeconds(
                            Long.parseLong(
                                    cdcParams.get("heartbeat.interval").toString())));
        }

        /* ------------------ connection ------------------ */
        if (cdcParams.containsKey("connect.timeout")) {
            sourceBuilder.connectTimeout(
                    Duration.ofSeconds(
                            Long.parseLong(cdcParams.get("connect.timeout").toString())));
        }

        if (cdcParams.containsKey("connect.max-retries")) {
            sourceBuilder.connectMaxRetries(
                    Integer.parseInt(cdcParams.get("connect.max-retries").toString()));
        }

        if (cdcParams.containsKey("connection.pool.size")) {
            sourceBuilder.connectionPoolSize(
                    Integer.parseInt(cdcParams.get("connection.pool.size").toString()));
        }

        /* ------------------ behavior flags ------------------ */
        if (cdcParams.containsKey("include.schema.changes")) {
            sourceBuilder.includeSchemaChanges(
                    Boolean.parseBoolean(
                            cdcParams.get("include.schema.changes").toString()));
        }

        if (cdcParams.containsKey("scan.incremental.close-idle-reader.enabled")) {
            sourceBuilder.closeIdleReaders(
                    Boolean.parseBoolean(
                            cdcParams.get("scan.incremental.close-idle-reader.enabled").toString()));
        }

        if (cdcParams.containsKey("scan.incremental.snapshot.backfill.skip")) {
            sourceBuilder.skipSnapshotBackfill(
                    Boolean.parseBoolean(
                            cdcParams.get("scan.incremental.snapshot.backfill.skip").toString()));
        }

        if (cdcParams.containsKey("scan.incremental.snapshot.chunk.key-column")){
            sourceBuilder.chunkKeyColumn(
                    cdcParams.get("scan.incremental.snapshot.chunk.key-column").toString()
            );
        }

        return sourceBuilder;
    }

    public MySqlSourceBuilder<BinarySourceRecord> mySqlSourceBuilder(String cdcConfigYamlPath, MySqlSourceBuilder<BinarySourceRecord> sourceBuilder) throws IOException {
        Path path = new Path(cdcConfigYamlPath);
        FileSystem fs = FileSystem.get(path.toUri());
        Map<String, Object> cdcParams;
        try (InputStream input = fs.open(path)) {
            cdcParams = loadAsFlatMap(input);
        }
        Properties debeziumProperties = getDebeziumProperties(cdcParams);
        Properties jdbcProperties = getJdbcProperties(cdcParams);
        if (!jdbcProperties.isEmpty()){
            sourceBuilder.jdbcProperties(jdbcProperties);
        }
        if (!debeziumProperties.isEmpty()){
            sourceBuilder.debeziumProperties(debeziumProperties);
        }
        if (cdcParams.containsKey(SCAN_SNAPSHOT_FETCH_SIZE.key())){
            sourceBuilder.fetchSize(Integer.parseInt(cdcParams.get(SCAN_SNAPSHOT_FETCH_SIZE.key()).toString()));
        }
        if (cdcParams.containsKey(SCAN_NEWLY_ADDED_TABLE_ENABLED.key())){
            sourceBuilder.scanNewlyAddedTableEnabled(Boolean.parseBoolean(cdcParams.get(SCAN_NEWLY_ADDED_TABLE_ENABLED.key()).toString()));
        }
        if (cdcParams.containsKey(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key()) && cdcParams.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key()) != null){
            String databaseName = cdcParams.get(DATABASE_NAME.key()).toString();
            String tableName = cdcParams.get(TABLE_NAME.key()).toString();
            sourceBuilder.chunkKeyColumn(new ObjectPath(databaseName, tableName), SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key());
        }
        if (cdcParams.containsKey(CHUNK_META_GROUP_SIZE.key())) {
            sourceBuilder.splitMetaGroupSize(
                    Integer.parseInt(cdcParams.get(CHUNK_META_GROUP_SIZE.key()).toString()));
        }
        if (cdcParams.containsKey(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key())) {
            sourceBuilder.distributionFactorLower(
                    Double.parseDouble(
                            cdcParams.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key()).toString()));
        }
        if (cdcParams.containsKey(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key())) {
            sourceBuilder.distributionFactorUpper(
                    Double.parseDouble(
                            cdcParams.get(CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key()).toString()));
        }
        if (cdcParams.containsKey(CONNECT_TIMEOUT.key())) {
            sourceBuilder.connectTimeout(
                    Duration.ofSeconds(
                            Long.parseLong(cdcParams.get(CONNECT_TIMEOUT.key()).toString())));
        }
        if (cdcParams.containsKey(CONNECT_MAX_RETRIES.key())) {
            sourceBuilder.connectMaxRetries(
                    Integer.parseInt(cdcParams.get(CONNECT_MAX_RETRIES.key()).toString()));
        }
        if (cdcParams.containsKey(CONNECTION_POOL_SIZE.key())) {
            sourceBuilder.connectionPoolSize(
                    Integer.parseInt(cdcParams.get(CONNECTION_POOL_SIZE.key()).toString()));
        }
        if (cdcParams.containsKey(SCAN_STARTUP_MODE.key())) {
            sourceBuilder.startupOptions(
                    parseMysqlStartupOptions(
                            cdcParams.get(SCAN_STARTUP_MODE.key()).toString()));
        }
        if (cdcParams.containsKey(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key())){
            sourceBuilder.splitSize(
                    Integer.parseInt(cdcParams.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE.key()).toString()));
        }
        if (cdcParams.containsKey(HEARTBEAT_INTERVAL.key())) {
            sourceBuilder.heartbeatInterval(
                    Duration.ofSeconds(
                            Long.parseLong(cdcParams.get(HEARTBEAT_INTERVAL.key()).toString())));
        }
        if (cdcParams.containsKey(SERVER_ID.key())){
            String serverId = (String) cdcParams.get(SERVER_ID.key());
            sourceBuilder.serverId(serverId);
        }
        if (cdcParams.containsKey(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.key())) {
            sourceBuilder.skipSnapshotBackfill(
                    Boolean.parseBoolean(
                            cdcParams.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP.key()).toString()));
        }
        if (cdcParams.containsKey(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.key())) {
            sourceBuilder.closeIdleReaders(
                    Boolean.parseBoolean(
                            cdcParams.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED.key()).toString()));
        }
        return sourceBuilder;
    }

    private com.ververica.cdc.connectors.mysql.table.StartupOptions parseMysqlStartupOptions(String mode) {
        switch (mode.toLowerCase()) {
            case "initial":
                return com.ververica.cdc.connectors.mysql.table.StartupOptions.initial();
            case "latest-offset":
            case "latest":
                return com.ververica.cdc.connectors.mysql.table.StartupOptions.latest();
            case "earliest-offset":
            case "earliest":
                return com.ververica.cdc.connectors.mysql.table.StartupOptions.earliest();
            default:
                throw new IllegalArgumentException(
                        "Unsupported scan.startup.mode: " + mode);
        }
    }

    private com.ververica.cdc.connectors.base.options.StartupOptions parsePgStartupOptions(String mode){
        switch (mode.toLowerCase()) {
            case "initial":
                return com.ververica.cdc.connectors.base.options.StartupOptions.initial();
            case "latest-offset":
            case "latest":
                return com.ververica.cdc.connectors.base.options.StartupOptions.latest();
            case "earliest-offset":
            case "earliest":
                return com.ververica.cdc.connectors.base.options.StartupOptions.earliest();
            default:
                throw new IllegalArgumentException(
                        "Unsupported scan.startup.mode: " + mode);
        }
    }

    private Properties getJdbcProperties(Map<String, Object> cdcParams){
        HashMap<String, String> jdbcParams = new HashMap<>();
        final String JDBC_PREFIX = "jdbc.properties.";
        for (Map.Entry<String, Object> entry : cdcParams.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key != null && key.startsWith(JDBC_PREFIX) && value != null) {
                String jdbcValue = String.valueOf(value);
                jdbcParams.put(key, jdbcValue);
            }
        }
        return JdbcUrlUtils.getJdbcProperties(jdbcParams);
    }

    private Properties getDebeziumProperties(Map<String, Object> cdcParams){
        HashMap<String, String> dbzParams = new HashMap<>();
        final String JDBC_PREFIX = "debezium.";
        for (Map.Entry<String, Object> entry : cdcParams.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key != null && key.startsWith(JDBC_PREFIX) && value != null) {
                String dbzValue = String.valueOf(value);
                dbzParams.put(key, dbzValue);
            }
        }
        return DebeziumOptions.getDebeziumProperties(dbzParams);
    }

    public HashMap<String, List<String>> getTablePartitionList(String cdcConfigPath) throws IOException {
        Path path = new Path(cdcConfigPath);
        FileSystem fs = FileSystem.get(path.toUri());
        try (InputStream input = fs.open(path)) {
            Map<String, Object> cdcParams = loadAsFlatMap(input);

            HashMap<String, List<String>> tablePartitions = new HashMap<>();
            String partitionPrefix = "partitions.";

            for (Map.Entry<String, Object> entry : cdcParams.entrySet()) {
                String key = entry.getKey();
                if (!key.startsWith(partitionPrefix)) {
                    continue;
                }

                String tableName = key.substring(partitionPrefix.length());
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }

                String cols = value.toString().trim();
                if (cols.isEmpty()) {
                    continue;
                }

                List<String> partitionCols = Arrays.stream(cols.split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());

                tablePartitions.put(tableName, partitionCols);
            }
            return tablePartitions;
        }
    }

    public HashMap<String, String> getTablePartitionFormatRuleList(String cdcConfigPath) throws IOException {
        Path path = new Path(cdcConfigPath);
        FileSystem fs = FileSystem.get(path.toUri());
        try (InputStream input = fs.open(path)) {
            Map<String, Object> cdcParams = loadAsFlatMap(input);

            HashMap<String, String> tablePartitionsFormatRules = new HashMap<>();
            String partitionPrefix = "timestamp-format-rule.";

            for (Map.Entry<String, Object> entry : cdcParams.entrySet()) {
                String key = entry.getKey();
                if (!key.startsWith(partitionPrefix)) {
                    continue;
                }

                String tableName = key.substring(partitionPrefix.length());
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }

                String cols = value.toString().trim();
                if (cols.isEmpty()) {
                    continue;
                }
                tablePartitionsFormatRules.put(tableName, cols);
            }
            return tablePartitionsFormatRules;
        }
    }
}