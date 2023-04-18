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
import org.junit.Test;


public class LakeSoulSourceTest {
    @Test
    public void lakeSoulSourceStreamTest() {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String testSql = "select * from user_info /*+ OPTIONS('readstarttime'='2023-04-19 10:00:00','readendtime'='2023-04-19 11:20:00','readtype'='incremental')*/";
        String testSql1 = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        String testSql2 = "select name,`date` from user_date where `date`='0320'";
        String testSql3 = "select order_id,`range`,name from user_range where `range`='0321'";
        String testSql4 = "select * from user_only_range where `range` = '0320'";
        String testSql5 = "select * from user_none";
        String testSql6 = "select name,op from `merge` where order_id='1'";
        String testSql7 = "select * from user_info as ui inner join order_info as oi on ui.order_id=oi.id order by oi.price";
        tEnvs.executeSql(testSql).print();
    }

    @Test
    public void lakeSoulSourceBatchTest(){
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String testSql = "select * from user_info /*+ OPTIONS('readstarttime'='2023-04-19 10:00:00','readendtime'='2023-04-19 11:20:00','readtype'='incremental')*/";
        String testSql1 = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        String testSql2 = "select name,`date` from user_date where `date`='0320'";
        String testSql3 = "select order_id,`range`,name from user_range where `range`='0321'";
        String testSql4 = "select * from user_only_range where `range` = '0320'";
        String testSql5 = "select * from user_none";
        String testSql6 = "select name,op from `merge` where order_id='1'";
        String testSql7 = "select * from user_info as ui inner join order_info as oi on ui.order_id=oi.id order by oi.price";
        String testSql8 = "select order_id from user_info";
        tEnvs.executeSql(testSql8).print();
    }
}
