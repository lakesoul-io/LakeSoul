package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.runtime.state.FileStateBackendTest;
import org.apache.flink.runtime.state.testutils.BackendForTestStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
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
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String testSql = "select * from user_info /*+ OPTIONS('readstarttime'='2023-04-21 10:00:00','readtype'='incremental')*/";
        String testSql1 = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        String testSql2 = "select name,`date` from user_date where `date`='0320'";
        String testSql3 = "select order_id,`range`,name from user_range where `range`='0321'";
        String testSql4 = "select * from user_only_range where `range` = '0320'";
        String testSql5 = "select * from user_none";
        String testSql6 = "select name,op from `merge` where order_id='1'";
        String testSql7 = "select * from user_info as ui inner join order_info as oi on ui.order_id=oi.id ";
        tEnvs.executeSql(testSql).print();
    }

    @Test
    public void lakeSoulSourceBatchTest() {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");

        String testSql = "select * from user_info /*+ OPTIONS('readendtime'='2023-04-21 10:33:00','lakesoul_cdc_change_column'='order_id')*/";
        String testSql1 = "select ui.order_id,sum(oi.price) as total_price,count(*) as total " +
                "from user_info as ui inner join order_info as oi " +
                "on ui.order_id=oi.id group by ui.order_id having ui.order_id>2";
        String testSql2 = "select * from user_date ";
        String testSql3 = "select order_id,`range`,name from user_range where `range`='0321'";
        String testSql4 = "select * from user_only_range where `range` = '0320'";
        String testSql5 = "select * from user_none";
        String testSql6 = "select * from `merge` /*+ OPTIONS('lakesoul_cdc_change_column'='op')*/";
        String testSql7 = "select * from user_info as ui inner join order_info as oi on ui.order_id=oi.id order by oi.price";
        String testSql8 = "select order_id from user_info";
        String testSql9 = "select id,score from user_multi_hash where `date`='0320'";
        String testSql10 = "select name,score,`date` from user_multi /*+ OPTIONS('readstarttime'='2023-04-21 10:00:00','readendtime'='2023-04-21 16:20:00','readtype'='incremental')*/";
        String testSql11 = "select * from user_cdc /*+ OPTIONS('lakesoul_cdc_change_column'='order_id')*/";
        tEnvs.executeSql(testSql11).print();
    }

    @Test
    public void lakeSoulStreamFailoverTest() {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(4023);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink/");

        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        String testSql = "select * from user_info /*+ OPTIONS('readendtime'='2023-04-23 10:00:00')*/";
        tEnvs.executeSql(testSql).print();
    }
}
