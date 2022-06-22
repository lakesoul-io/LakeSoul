package org.apache.flink.lakesoul.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.metaData.DataInfo;
import org.apache.flink.lakesoul.metaData.LakesoulCatalog;
import org.apache.flink.lakesoul.sink.LakesoulFileSink;
import org.apache.flink.lakesoul.sink.LakesoulFileWriter;
import org.apache.flink.lakesoul.sink.LakesoulTableSink;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class flinkCdcTest {

    private StreamTableEnvironment tEnvs;
    private StreamExecutionEnvironment env;
    private final String LAKESOUL = "lakesoul";
    private Configuration conf;


    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("_id", DataTypes.STRING().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("_id")));


    @Before
    public void before() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1001);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30003);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/zhyang/Downloads/flink");
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);

        Catalog lakesoulCatalog = new LakesoulCatalog();
        tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
    }

//    @Test
//    public void cdcTest() throws Exception {
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("yourHostname")
//                .port(3306)
//                .databaseList("yourDatabaseName") // set captured database
//                .tableList("yourDatabaseName.yourTableName") // set captured table
//                .username("yourUsername")
//                .password("yourPassword")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .build();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        DataStreamSource<String> datastream = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//
//
//            LakesoulFileWriter<RowData> fileWriter =
//                    new LakesoulFileWriter<>(
//                            Duration.ofMinutes(1).toMillis(), bucketsBuilder, " ", conf, outputFile);
//
//
//                LakesoulFileSink.forBulkFormat(
//                                new Path(),
//                                new LakesoulTableSink.ProjectionBulkFactory(
//                                        (BulkWriter.Factory<RowData>) writer, computer))
//                        .withBucketAssigner(assigner)
//                        .withOutputFileConfig(fileNamingConfig)
//                        .withRollingPolicy(lakesoulPolicy);
//
//
//            datastream.transform(LakesoulFileWriter.class.getSimpleName(),
//                        TypeInformation.of(DataInfo.class),
//                        fileWriter).name( "DataInfo" )
//                .setParallelism(1);
//
//
//    }


}
