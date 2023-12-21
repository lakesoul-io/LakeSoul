// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//package org.apache.flink.lakesoul.entry;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import com.ververica.cdc.connectors.mongodb.source.MongoDBSource;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//
//public class MongodbCDC {
//
//    public static void main(String[] args) throws Exception {
//        MongoDBSource<String> mongoSource =
//                MongoDBSource.<String>builder()
//                        .hosts("localhost:27017")
//                        .databaseList("test") // set captured database, support regex
//                        .collectionList("test.sms") //set captured collections, support regex
//                        .username("flink")
//                        .password("flinkpw")
//                        .deserializer(new JsonDebeziumDeserializationSchema())
//                        .build();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // enable checkpoint
//        env.enableCheckpointing(3000);
//        // set the source parallelism to 2
//        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "MongoDBIncrementalSource")
//                .setParallelism(2)
//                .print()
//                .setParallelism(1);
//
//        env.execute("Print MongoDB Snapshot + Change Stream");
//    }
//}
