package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.LakesoulCatalog;
import org.apache.flink.lakesoul.LakesoulFileSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;


public class LakeSoulFileSinkTest {

    private StreamTableEnvironment tEnvs;
    private StreamExecutionEnvironment env;
    private final String LAKESOUL = "lakesoul";
    private DBManager DbManage;


    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);
        tEnvs = StreamTableEnvironment.create(env);
        tEnvs.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tEnvs.registerCatalog(LAKESOUL,lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
        DbManage = new DBManager();
    }




    @Test
    public void createStreamingSinkTest() throws InterruptedException {
        //   tableEnv.executeSql( "CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with('connector' = 'lakesoul','format'='parquet','path'='D://lakesoultest','lakesoul_cdc'='true')" );

        tEnvs.executeSql( "CREATE TABLE user_b111223 ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('connector' = 'lakesoul','format'='parquet','path'='/Users/zhyang/Downloads','lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043','lakesoul_cdc'='true')" );
            tEnvs.executeSql("insert into user_b111223 values (1666,'key1','value1'),(2666,'key1','value2'),(3666,'key3','value3')");
//            tEnvs.executeSql("insert into user_b values (11,'key1','value1'),(22,'key1','value2'),(33,'key3','value3')");
//            tEnvs.executeSql("insert into user_b values (1119,'key19','value1'),(222,'key91','value2'),(3393,'key973','value3')");

    }


    }





