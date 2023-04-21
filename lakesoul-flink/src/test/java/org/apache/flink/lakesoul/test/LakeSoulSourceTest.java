package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.junit.Test;

import java.io.IOException;

public class LakeSoulSourceTest {
    @Test
    public void flinkLakeSoulSourceTest() throws TableNotPartitionedException, TableNotExistException, IOException {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String testSql = "select * from user_info";
        String testSql1 = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        String testSql2 = "select name,`date` from user_date where `date`='0320'";
        String testSql3 = "select order_id,`range`,name from user_range where `range`='0321'";
        String testSql4 = "select * from user_only_range where `range` = '0320'";
        String testSql5 = "select * from user_none";
        String testSql6 = "select * from `merge` where order_id='2'";
        tEnvs.executeSql(testSql6).print();
    }

    @Test
    public void flinkLakeSoulSinkTest() throws Exception {
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        LakeSoulCatalog lakesoulCatalog = new LakeSoulCatalog();
        batchEnv.registerCatalog(lakesoulCatalog.getName(), lakesoulCatalog);
        batchEnv.useCatalog(lakesoulCatalog.getName());
        // create the lakesoul non-partitioned table
        batchEnv.executeSql(
                String.format(
                        "create table partition_hash_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string, primary key(x) not enforced) "
//                                "partitioned by ("
//                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='','%s'='5min', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/partition_hash_table"));
        batchEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        batchEnv.executeSql(
                        "insert overwrite partition_hash_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31')")
//                                + "(2,'b',22,2020,'08','31')")
                .await();
        batchEnv.executeSql("select * from partition_hash_table").print();
    }

}
