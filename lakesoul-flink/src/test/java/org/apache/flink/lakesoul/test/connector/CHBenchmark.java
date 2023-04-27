package org.apache.flink.lakesoul.test.connector;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

public class CHBenchmark {


    public static final int customRecordNum =200;

    public static final int customerNum=50;

    public static final int updateCustomerInterval = 2 * 1000;

    public static final int orderNumPerBatch = customRecordNum;

    public static final int newOrderInterval = 10*1000;

    public static final int insertTimes = 6;

    private static String lookupTtl="10s";


    static final String[] drop_table = {
//            "DROP TABLE IF EXISTS customer",
            "DROP TABLE IF EXISTS oorder",
            "DROP TABLE IF EXISTS bounded_table",
    };

    static final String[] create_table = {
//            COLLECTION Table Source For test
            "CREATE TABLE default_catalog.default_database.customer (\n" +
            "    c_w_id         int            NOT NULL,\n" +
            "    c_d_id         int            NOT NULL,\n" +
            "    c_id           int            NOT NULL,\n" +
//            "    c_discount     decimal(4, 4)  NOT NULL,\n" +
//            "    c_credit       string        NOT NULL,\n" +
//            "    c_last         string    NOT NULL,\n" +
//            "    c_first        string    NOT NULL,\n" +
//            "    c_credit_lim   decimal(12, 2) NOT NULL,\n" +
//            "    c_balance      decimal(12, 2) NOT NULL,\n" +
//            "    c_ytd_payment  float          NOT NULL,\n" +
//            "    c_payment_cnt  int            NOT NULL,\n" +
//            "    c_delivery_cnt int            NOT NULL,\n" +
//            "    c_street_1     string    NOT NULL,\n" +
//            "    c_street_2     string    NOT NULL,\n" +
//            "    c_city         string    NOT NULL,\n" +
//            "    c_state        string        NOT NULL,\n" +
//            "    c_zip          string       NOT NULL,\n" +
//            "    c_phone        string       NOT NULL,\n" +
////            "    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
//            "    c_since        timestamp      NOT NULL,\n" +
//            "    c_middle       string        NOT NULL,\n" +
//            "    c_data         string   NOT NULL,\n" +
            "    proctime         as proctime() , \n" +
//            "    PRIMARY KEY (c_w_id, c_d_id, c_id) \n" +
            "    PRIMARY KEY (c_w_id, c_d_id, c_id) NOT ENFORCED\n" +
            ")" +
            "WITH (" +
                    "'connector'='COLLECTION','is-bounded' = 'false'," +
            String.format("'path'='%s'", "tmp/customer") +
            ")",

            //            COLLECTION Table Source For test
            "CREATE TABLE default_catalog.default_database.bounded_customer (\n" +
                    "    c_w_id         int            NOT NULL,\n" +
                    "    c_d_id         int            NOT NULL,\n" +
                    "    c_id           int            NOT NULL,\n" +
//            "    c_discount     decimal(4, 4)  NOT NULL,\n" +
//            "    c_credit       string        NOT NULL,\n" +
//            "    c_last         string    NOT NULL,\n" +
//            "    c_first        string    NOT NULL,\n" +
//            "    c_credit_lim   decimal(12, 2) NOT NULL,\n" +
//            "    c_balance      decimal(12, 2) NOT NULL,\n" +
//            "    c_ytd_payment  float          NOT NULL,\n" +
//            "    c_payment_cnt  int            NOT NULL,\n" +
//            "    c_delivery_cnt int            NOT NULL,\n" +
//            "    c_street_1     string    NOT NULL,\n" +
//            "    c_street_2     string    NOT NULL,\n" +
//            "    c_city         string    NOT NULL,\n" +
//            "    c_state        string        NOT NULL,\n" +
//            "    c_zip          string       NOT NULL,\n" +
//            "    c_phone        string       NOT NULL,\n" +
////            "    c_since        timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
//            "    c_since        timestamp      NOT NULL,\n" +
//            "    c_middle       string        NOT NULL,\n" +
//            "    c_data         string   NOT NULL,\n" +
//            "    PRIMARY KEY (c_w_id, c_d_id, c_id) \n" +
                    "    PRIMARY KEY (c_w_id, c_d_id, c_id) NOT ENFORCED\n" +
                    ")" +
                    "WITH (" +
                    "'connector'='COLLECTION','is-bounded' = 'true'," +
                    String.format("'path'='%s'", "tmp/bounded_customer") +
                    ")",
            "CREATE TABLE oorder (\n" +
            "    o_w_id       int       NOT NULL,\n" +
            "    o_d_id       int       NOT NULL,\n" +
            "    o_id         int       NOT NULL,\n" +
            "    o_c_id       int       NOT NULL,\n" +
////                    "    o_carrier_id int                DEFAULT NULL,\n" +
//            "    o_carrier_id int                ,\n" +
//            "    o_ol_cnt     int       NOT NULL,\n" +
//            "    o_all_local  int       NOT NULL,\n" +
////                    "    o_entry_d    timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
//            "    o_entry_d    timestamp NOT NULL,\n" +
            "    PRIMARY KEY (o_w_id, o_d_id, o_id) NOT ENFORCED\n" +
//                    "    UNIQUE (o_w_id, o_d_id, o_c_id, o_id" +
            ")"  +
            "WITH (" +
            String.format("'format'='lakesoul','path'='%s', '%s'='%s', '%s'='false' ", "tmp/oorder", JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), lookupTtl,LakeSoulSinkOptions.USE_CDC.key()) +
            ")",
//            "create table default_catalog.default_database.probe (x int,y string, p as proctime()) "
//                    + "with ('connector'='COLLECTION','is-bounded' = 'false')",
//            String.format("create table bounded_table (x int, y string, z int) with ('format'='','%s'='5min', 'path'='%s')",
//            JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/bounded_table"),
            "CREATE TABLE default_catalog.default_database.sink (" +
                    "    c_id           int            NOT NULL,\n" +
                    "    c_count        BIGINT            NOT NULL\n" +
                    ")" +
            "WITH (" +
            "'connector' = 'values', 'sink-insert-only' = 'false'" +
            ")",
    };

    static final String query_0 = "select p.c_w_id, p.c_d_id, b.o_id, b.o_c_id from "
//            + " default_catalog.default_database.probe as p "
            + "default_catalog.default_database.customer as p"
            + " join oorder for system_time as of p.proctime as b on p.c_w_id=b.o_w_id and p.c_d_id=b.o_d_id and p.c_id=b.o_c_id";

    static final String streaming_query_13 =
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

    static final String batch_query_13 =
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
        LakeSoulCatalog lakeSoulCatalog;


        private String tableName = "oorder";

        public NewOrderThread(LakeSoulCatalog lakeSoulCatalog) {
            this.lakeSoulCatalog = lakeSoulCatalog;
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
            int orderNum = 0;
            for (int i = 0; i < insertTimes; i++) {
                List<String> valueList =
                        new ArrayList<>();
                for (int j = 0; j < orderNumPerBatch; j++) {
                    // value with schema(o_w_id, o_d_id, o_id, o_c_id)
                    //valueList.add(String.format("(1, 1, %s, %s)", orderNum++, Random.randInt(customerNum)));
                    valueList.add(String.format("(1, 1, %s, %s)", orderNum++, j));
                }
                String values = String.join(",",valueList);
                String insertSql = String.format("insert into %s values %s", tableName, values);
                batchEnv.executeSql(insertSql);
                try {
                    Thread.sleep(newOrderInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
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
        NewOrderThread newOrderThread = new NewOrderThread(lakeSoulCatalog);
        newOrderThread.start();
        tableEnv.executeSql("INSERT INTO default_catalog.default_database.sink " + streaming_query_13).await();
        List<String> results = TestValuesTableFactory.getResults("sink");

        System.out.println(results);

//        initCollectionSourceData(-1);
//        newOrderThread.join();
//        batchEnv.executeSql(create_table[1]);
//        System.out.println(CollectionUtil.iteratorToList(batchEnv.executeSql(batch_query_13).collect()));
    }
}
