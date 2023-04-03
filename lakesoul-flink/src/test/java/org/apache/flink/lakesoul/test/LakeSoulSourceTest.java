package org.apache.flink.lakesoul.test;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.source.*;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.RowData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LakeSoulSourceTest {
    @Test
    public void flinkLakeSoulSourceTest() {
        StreamTableEnvironment tEnvs;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        Catalog lakesoulCatalog = new LakeSoulCatalog();
        String lakeTablePath = "file:///tmp/lakesoul";
        List<Path> parquetList = new ArrayList<>();
        parquetList.add(new Path(lakeTablePath));
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase("default");
        String tableName = "lakeTable";

//        LakeSoulSplit split = new LakeSoulSplit("1", parquetList);
//        LakeSoulSimpleSplitAssigner assigner = new LakeSoulSimpleSplitAssigner(Arrays.asList(split));
//        LakeSoulSource lakesoulSource = new LakeSoulSource(new TableId(lakesoulCatalog.getDefaultDatabase(), null, "lakeSource"), null);
//        DataStream<RowData> stream = env.fromSource(lakesoulSource, WatermarkStrategy.noWatermarks(), "LakeSoulSourceTest");

        String sqlQuery = "select * from " + tableName;
        tEnvs.executeSql(sqlQuery);
    }
}
