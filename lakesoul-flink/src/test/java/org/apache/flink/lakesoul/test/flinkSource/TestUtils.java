package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class TestUtils {

    private static String BATCH_TYPE = "batch";
    private static String STREAMING_TYPE = "streaming";
    public static TableEnvironment createTableEnv(String mode) {
        TableEnvironment createTableEnv;
        if(mode.equals(BATCH_TYPE)) {
            createTableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        }else{
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            createTableEnv = StreamTableEnvironment.create(env);
        }
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        createTableEnv.registerCatalog("lakeSoul", lakesoulCatalog);
        createTableEnv.useCatalog("lakeSoul");
        createTableEnv.useDatabase("default");
        return createTableEnv;
    }

    public static StreamTableEnvironment createStreamTableEnv(String envType) {
        StreamTableEnvironment tEnvs;
        Configuration conf = new Configuration();
        Random random = new Random();
        int randomNumber = random.nextInt(1001) + 9181;
        conf.setInteger("rest.port", randomNumber);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        if (envType.equals(STREAMING_TYPE)) {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        }
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        return tEnvs;
    }

    public static String getDateTimeFromTimestamp(Instant instant){
        ZoneId zoneId = ZoneId.of("Africa/Accra");
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return zonedDateTime.format(formatter);
    }

    public static void checkEqualInAnyOrder(List<Row> results, String[] expectedResult) {
        assertThat(results.stream().map(row -> row.toString()).collect(Collectors.toList())).
                containsExactlyInAnyOrder(expectedResult);
    }

    public static void createLakeSoulSourceTableUser(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table user_info (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    score DECIMAL" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2',"+
                "    'path'='/tmp/lakeSource/user' )";
        tEnvs.executeSql("DROP TABLE if exists user_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80), (3, 'Jack', 75), (3, 'Amy', 95),(5, 'Tom', 75), (4, 'Mike', 70)").await();
    }

    public static void createLakeSoulSourceMultiPartitionTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createSql = "create table user_multi (" +
                "    `id` INT," +
                "    name STRING," +
                "    score INT," +
                "    `date` DATE," +
                "    region STRING," +
                "PRIMARY KEY (`id`,`name`) NOT ENFORCED" +
                ") " +
                "PARTITIONED BY (`region`,`date`)" +
                "WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/multi_range_hash' )";
        tEnvs.executeSql("DROP TABLE if exists user_multi");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql("INSERT INTO user_multi VALUES (1, 'Bob', 90, TO_DATE('1995-10-01'), 'China'), (2, 'Alice', 80, TO_DATE('1995-10-10'), 'China'), " +
                "(3, 'Jack', 75,  TO_DATE('1995-10-15'), 'China'), (3, 'Amy', 95,  TO_DATE('1995-10-10'),'UK'), " +
                "(5, 'Tom', 75,  TO_DATE('1995-10-01'), 'UK'), (4, 'Mike', 70, TO_DATE('1995-10-15'), 'UK')").await();
    }

    public static void createLakeSoulSourceTableOrder(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createOrderSql = "create table order_info (" +
                "    `id` INT PRIMARY KEY NOT ENFORCED," +
                "    price DOUBLE" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/order' )";
        tEnvs.executeSql("DROP TABLE if exists order_info");
        tEnvs.executeSql(createOrderSql);
        tEnvs.executeSql("INSERT INTO order_info VALUES (1, 20.12), (2, 10.88), (3, 15.35), (4, 25.24), (5, 15.04)").await();
    }
}
