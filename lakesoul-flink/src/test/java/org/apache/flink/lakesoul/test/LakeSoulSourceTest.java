package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.junit.Test;

public class LakeSoulSourceTest {
    @Test
    public void flinkLakeSoulSourceTest() {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        String tableName = "lakeTable";
        String sqlQuery = "select * from " + tableName;
        tEnvs.executeSql(sqlQuery).print();
    }
}
