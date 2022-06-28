/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.*;
import org.apache.flink.lakesoul.LakesoulCatalogFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.api.Table;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.types.Row;
import org.apache.flink.connector.file.sink.FileSink;
import org.junit.Before;
import org.junit.Test;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
public class Lakesoul {
    private Map<String, String> props;

    @Before
    public void before() {
        props =new HashMap<String,String>();
        props.put("type", "lakesoul");
    }
    @Test
    public void LakesoulCatalog(){
        LakesoulCatalogFactory catalogFactory =new LakesoulCatalogFactory();
        Catalog lakesoulCatalog = catalogFactory.createCatalog("lakesoul",props);
        assertTrue(lakesoulCatalog instanceof LakesoulCatalog);
    }
    @Test
    public void registerCatalog(){
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tableEnv.registerCatalog("lakesoul",lakesoulCatalog);
        tableEnv.useCatalog("lakesoul");
        System.out.println(tableEnv.getCurrentCatalog());
        assertEquals(true, tableEnv.getCatalog("lakesoul").get() instanceof LakesoulCatalog);
    }

    @Test
    public void tableSink(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
 //       TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");
        Table inputTable = tableEnv.fromDataStream(dataStream);
        tableEnv.createTemporaryView("InputTable", inputTable);
        Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        String outputPath="D:\\Lakesoul";
        FileSink<String> sink = FileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>())
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(10)
                                .withInactivityInterval(10)
                                .withMaxPartSize(1000)
                                .build())
                .build();

        resultStream.addSink((SinkFunction<Row>) sink);

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Test
    public void createTable(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tableEnv.registerCatalog("lakesoul",lakesoulCatalog);
        tableEnv.useCatalog("lakesoul");
        tableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.executeSql( "CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with ('lakesoul_cdc_change_column'='name','lakesoul_meta_host'='127.0.0.2','lakesoul_meta_host_port'='9043')" );
    }

    @Test
    public void sqlTablesink(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
      // env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().set(
                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
//        tableEnv.getConfig().getConfiguration().set(
//                ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(1));
        tableEnv.executeSql("drop table user_behavior");
       String createSql="CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) WITH ( 'connector' = 'lakesoul')";
   //     String createSql="CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) WITH ( 'connector' = 'filesystem', 'path' = 'file:///D/lakesoultest', 'format' = 'parquet')";
        tableEnv.executeSql(createSql);
        tableEnv.executeSql("insert into user_behavior values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
        //ableEnv.executeSql("insert into user_behavior values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
        tableEnv.executeSql("insert into user_behavior values (4,'key1','value1'),(5,'key1','value2'),(6,'key3','value3')");
        //tableEnv.sqlQuery("select * from user_behavior").execute().print();
        //tableEnv.executeSql("select * from user_behavior").print();
        //System.out.println(explainstr);
//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
    @Test
    public void sqlLakesoulTablesink(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tableEnv.registerCatalog("lakesoul",lakesoulCatalog);
        tableEnv.useCatalog("lakesoul");
//        tableEnv.getConfig().getConfiguration().set(
//                ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
     //   tableEnv.executeSql( "CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with('connector' = 'lakesoul','format'='parquet','path'='D://lakesoultest','lakesoul_cdc'='true')" );
        tableEnv.executeSql("insert into user_behavior values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
        tableEnv.executeSql("insert into user_behavior values (11,'key1','value1'),(22,'key1','value2'),(33,'key3','value3')");
        tableEnv.executeSql("insert into user_behavior values (111,'key1','value1'),(222,'key1','value2'),(333,'key3','value3')");
    }

    @Test
    public void sqlLakesoulTableBatchsink(){
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        Catalog lakesoulCatalog = new LakesoulCatalog();
        tableEnv.registerCatalog("lakesoul",lakesoulCatalog);
        tableEnv.useCatalog("lakesoul");
        tableEnv.executeSql("insert into user_behavior values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
        tableEnv.executeSql("insert into user_behavior values (11,'key1','value1'),(22,'key1','value2'),(33,'key3','value3')");
        tableEnv.executeSql("insert into user_behavior values (111,'key1','value1'),(222,'key1','value2'),(333,'key3','value3')");
    }
    @Test
    public void sqlDefaultsink(){
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.enableCheckpointing(10);
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bbSettings);
        //StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql( "CREATE TABLE user_behavior ( user_id BIGINT, dt STRING, name STRING,primary key (user_id) NOT ENFORCED ) PARTITIONED BY (dt) with('connector' = 'filesystem','format'='parquet','path'='D://lakesoultest')" );
        tableEnv.executeSql("insert into user_behavior values (1,'key1','value1'),(2,'key1','value2'),(3,'key3','value3')");
    }
}