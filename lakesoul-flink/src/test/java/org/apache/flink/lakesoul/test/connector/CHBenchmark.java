package org.apache.flink.lakesoul.test.connector;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

public class CHBenchmark {


    public static final int customerBatchSize = 100;

    public static final int customerBatchNum = 4;

    public static final int customRecordNum = customerBatchNum * customerBatchSize;

    public static final int updateCustomerInterval = 20 * 1000;

    public static final int orderNumPerBatch = customRecordNum;

    public static final int newOrderInterval = 10*1000;

    public static final int tailCustomRecordNum = customRecordNum / 3;

    public static final int insertTimes = (customRecordNum - tailCustomRecordNum) / customerBatchSize * updateCustomerInterval / newOrderInterval - 1;

    private static final String lookupTtl = "10s";

    private static final Random rand = new Random();


    static final String[] drop_table = {
            "DROP TABLE IF EXISTS customer",
            "DROP TABLE IF EXISTS oorder",
            "DROP TABLE IF EXISTS bounded_table",
    };

    static final String[] create_table = {
            // Streaming COLLECTION Table Source For test
            "CREATE TABLE default_catalog.default_database.customer (\n" +
            "    c_w_id         int            NOT NULL,\n" +
            "    c_d_id         int            NOT NULL,\n" +
            "    c_id           int            NOT NULL,\n" +
            "    proctime         as proctime() , \n" +
            "    PRIMARY KEY (c_w_id, c_d_id, c_id) NOT ENFORCED\n" +
            ")" +
            "WITH (" +
                    "'connector'='COLLECTION','is-bounded' = 'false'" +
            ")",

            //  Batch COLLECTION Table Source For test
            "CREATE TABLE default_catalog.default_database.bounded_customer (\n" +
                    "    c_w_id         int            NOT NULL,\n" +
                    "    c_d_id         int            NOT NULL,\n" +
                    "    c_id           int            NOT NULL,\n" +
                    "    PRIMARY KEY (c_w_id, c_d_id, c_id) NOT ENFORCED\n" +
                    ")" +
                    "WITH (" +
                    "'connector'='COLLECTION','is-bounded' = 'true'" +
                    ")",

            // Streaming COLLECTION Table Source For test
            "CREATE TABLE customer (\n" +
                "    c_w_id         int            NOT NULL,\n" +
                "    c_d_id         int            NOT NULL,\n" +
                "    c_id           int            NOT NULL,\n" +
                "    PRIMARY KEY (c_w_id, c_d_id, c_id) NOT ENFORCED\n" +
                ")" +
                "WITH (" +
                String.format("'format'='lakesoul','path'='%s', '%s'='%s', '%s'='false' ", "tmp/customer", JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), lookupTtl,LakeSoulSinkOptions.USE_CDC.key()) +
                ")",

            // LakeSoul Lookup Table
            "CREATE TABLE oorder (\n" +
            "    o_w_id       int       NOT NULL,\n" +
            "    o_d_id       int       NOT NULL,\n" +
            "    o_id         int       NOT NULL,\n" +
            "    o_c_id       int       NOT NULL,\n" +

            "    PRIMARY KEY (o_w_id, o_d_id, o_id) NOT ENFORCED\n" +
            ")"  +
            "WITH (" +
            String.format("'format'='lakesoul','path'='%s', '%s'='%s', '%s'='false' ", "tmp/oorder", JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), lookupTtl,LakeSoulSinkOptions.USE_CDC.key()) +
            ")",

            "CREATE TABLE default_catalog.default_database.sink (" +
                    "    c_id           int            NOT NULL,\n" +
                    "    c_count        BIGINT            NOT NULL\n" +
                    ")" +
            "WITH (" +
            "'connector' = 'values', 'sink-insert-only' = 'false'" +
            ")",
    };

    static final String query_13_collection_streaming =
//            "SELECT\n" +
//            "   c_count, count(*) AS custdist\n" +
//            "FROM (\n" +
                "SELECT " +
//                        "/*+ LOOKUP('table'='oorder', 'async'='false') */" +
                        "c_id, count(o_id) AS c_count \n" +
                "FROM " +
                    "default_catalog.default_database.customer\n" +
                    "LEFT OUTER JOIN `oorder` " +
                        "for system_time as of proctime\n" +
                    "ON (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id " +
//                    "AND o_carrier_id > 8" +
                    ")\n" +
                "GROUP BY c_id" +
//                        ") AS c_orders\n" +
//            "GROUP BY c_count\n" +
            "";
//                    +
//            "ORDER BY custdist DESC, c_count DESC";

    static final String query_13_lakesoul_streaming =
//            "SELECT\n" +
//            "   c_count, count(*) AS custdist\n" +
//            "FROM (\n" +
            "SELECT " +
//                        "/*+ LOOKUP('table'='oorder', 'async'='false') */" +
                    "c_id, count(o_id) AS c_count \n" +
                    "FROM " +
                    "(SELECT *, proctime() as proctime FROM customer) as c\n" +
                    "LEFT OUTER JOIN `oorder` " +
                    "for system_time as of c.proctime\n" +
                    "ON (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id " +
//                    "AND o_carrier_id > 8" +
                    ")\n" +
                    "GROUP BY c_id" +
//                        ") AS c_orders\n" +
//            "GROUP BY c_count\n" +
                    "";
//                    +
//            "ORDER BY custdist DESC, c_count DESC";

    static final String query_13_batch =
//            "SELECT\n" +
//            "   c_count, count(*) AS custdist\n" +
//            "FROM (\n" +
            "SELECT " +
//                        "/*+ LOOKUP('table'='oorder', 'async'='false') */" +
                    "c_id, count(o_id) AS c_count \n" +
                    "FROM " +
                    "default_catalog.default_database.bounded_customer\n" +
                    "LEFT OUTER JOIN `oorder` " +
                    "ON (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id " +
//                    "AND o_carrier_id > 8" +
                    ")\n" +
                    "GROUP BY c_id" +
//                        ") AS c_orders\n" +
//            "GROUP BY c_count\n" +
                    "";
//                    +
//            "ORDER BY custdist DESC, c_count DESC";

    static class NewOrderThread extends Thread {

        transient TableEnvironment batchEnv;
        final LakeSoulCatalog lakeSoulCatalog;

        final int interval;

        final int batchNum;

        final int batchSize;

        final String format;

        final String tableName;
        final int maxRepetition;

        public NewOrderThread(LakeSoulCatalog lakeSoulCatalog, String tableName, int interval, int batchNum, int batchSize, String format, int maxRepetition) {
            super();
            this.lakeSoulCatalog = lakeSoulCatalog;
            this.tableName = tableName;
            this.interval = interval;
            this.batchNum = batchNum;
            this.batchSize = batchSize;
            this.format = format;
            this.maxRepetition = maxRepetition;
        }

        @Override
        public void run() {
            batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
            batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
            batchEnv.useCatalog(lakeSoulCatalog.getName());

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            int counter = 0;
            for (int i = 0; i < batchNum; i++) {
                List<String> valueList =
                        new ArrayList<>();
                for (int j = 0; j < batchSize; j++) {
                    int rep = rand.nextInt(maxRepetition) + 1;
                    for (int r = 0; r < rep; r++)
                        // value with schema(o_w_id, o_d_id, o_id, o_c_id)
                        valueList.add(String.format(format, counter++, j));
                }
                String values = String.join(",",valueList);
                String insertSql = String.format("insert into %s values %s", tableName, values);
                batchEnv.executeSql(insertSql);
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
            System.out.println(String.format("Insert Into %s Done", tableName));
        }
    }

    public static void initCollectionSourceData(int updateCustomerInterval) {
        List<Row> data =
                new ArrayList<>();
        for (int i = 0; i < customRecordNum; i++) {
            // value with schema(c_w_id, c_d_id, c_id)
//            data.add(Row.of(1, 1, Random.randInt(customerNum)));
            data.add(Row.of(1, 1, i));
        }
        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(data, new ArrayList<>(), updateCustomerInterval);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second


        Configuration conf = new Configuration();
        StreamExecutionEnvironment env;

        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        ParameterTool pt = ParameterTool.fromMap(conf.toMap());
        env.getConfig().setGlobalJobParameters(pt);

        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);

        CheckpointingMode checkpointingMode = CheckpointingMode.EXACTLY_ONCE;
        if (parameter.get(JOB_CHECKPOINT_MODE.key(), JOB_CHECKPOINT_MODE.defaultValue()).equals("AT_LEAST_ONCE")) {
            checkpointingMode = CheckpointingMode.AT_LEAST_ONCE;
        }
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.getCheckpointConfig().setCheckpointStorage(parameter.get(FLINK_CHECKPOINT.key()));
        conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        LakeSoulCatalog lakeSoulCatalog = new LakeSoulCatalog();
        tableEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tableEnv.useCatalog(lakeSoulCatalog.getName());

        Arrays.stream(drop_table).forEach(tableEnv::executeSql);
        Arrays.stream(create_table).forEach(tableEnv::executeSql);


        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());

        initCollectionSourceData(updateCustomerInterval);
        NewOrderThread newOrderThread = new NewOrderThread(lakeSoulCatalog, "oorder", newOrderInterval, insertTimes, orderNumPerBatch, "(1, 1, %s, %s)", 5);
        newOrderThread.start();
//        tableEnv.executeSql("INSERT INTO default_catalog.default_database.sink " + query_13_collection_streaming).await();

        NewOrderThread newCustomerThread = new NewOrderThread(lakeSoulCatalog, "customer", updateCustomerInterval, customerBatchNum, customerBatchSize, "(1, 1, %s)", 1);
        newCustomerThread.start();

        try {
            tableEnv.executeSql("INSERT INTO default_catalog.default_database.sink " + query_13_lakesoul_streaming).await(updateCustomerInterval * customerBatchNum + 10000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            System.out.println("Join is done.");
        }
        List<String> results = TestValuesTableFactory.getResults("sink");
        results.sort(Comparator.comparing(row->Integer.valueOf(row.substring(3, row.indexOf(",")))));

        System.out.println(results);

        initCollectionSourceData(-1);
        try {
            newOrderThread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //Register batch COLLECTION TABLE for batchEnv
        batchEnv.executeSql(create_table[1]);
        List<Row> batchResult = CollectionUtil.iteratorToList(batchEnv.executeSql(query_13_batch).collect());
        batchResult.sort(Comparator.comparing(row -> row.getFieldAs(0)));
        System.out.println(batchResult);
        assertThat(batchResult.toString()).isNotEqualTo(results.toString());
        assertThat(batchResult.subList(customRecordNum - tailCustomRecordNum, customRecordNum).toString()).isEqualTo(results.subList(customRecordNum - tailCustomRecordNum, customRecordNum).toString());
        System.out.println("Assertion Pass");
        System.exit(0);
   }
}
