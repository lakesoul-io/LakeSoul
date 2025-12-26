package org.apache.flink.lakesoul.entry;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.table.catalog.ObjectPath;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import com.ververica.cdc.debezium.table.DebeziumOptions;
import com.ververica.cdc.debezium.utils.JdbcUrlUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions.*;

public class MysqlSourceBuilderTool {

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

    public MySqlSourceBuilder<BinarySourceRecord> mySqlSourceBuilder(String cdcConfigYamlPath, MySqlSourceBuilder<BinarySourceRecord> sourceBuilder) throws IOException {
        InputStream input =
                Files.newInputStream(Paths.get(cdcConfigYamlPath));
        Map<String, Object> cdcParams = loadAsFlatMap(input);
        Properties debeziumProperties = getDebeziumProperties(cdcParams);
        Properties jdbcProperties = getJdbcProperties(cdcParams);
        if (!jdbcProperties.isEmpty()){
            sourceBuilder.jdbcProperties(jdbcProperties);
        }
        if (!debeziumProperties.isEmpty()){
            sourceBuilder.debeziumProperties(jdbcProperties);
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
                    parseStartupOptions(
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

    private StartupOptions parseStartupOptions(String mode) {
        switch (mode.toLowerCase()) {
            case "initial":
                return StartupOptions.initial();
            case "latest-offset":
            case "latest":
                return StartupOptions.latest();
            case "earliest-offset":
            case "earliest":
                return StartupOptions.earliest();
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
}
