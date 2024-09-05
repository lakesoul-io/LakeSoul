package com.dmetasoul.lakesoul;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class SinkMemoryLeakTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(60_000);
        TableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        tableEnvironment.executeSql("create catalog lakesoul with ('type'='lakesoul')").await();
        tableEnvironment.executeSql(
                "CREATE TABLE source_table (\n" +
                        "    c0 STRING,\n" +
                        "    c1 STRING,\n" +
                        "    c2 STRING,\n" +
                        "    c3 STRING,\n" +
                        "    c4 STRING,\n" +
                        "    c5 STRING,\n" +
                        "    c6 STRING,\n" +
                        "    c7 STRING,\n" +
                        "    c8 STRING,\n" +
                        "    c9 STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'datagen',\n" +
                        "  'rows-per-second' = '20000'\n" +
                        ")").await();
        tableEnvironment.executeSql(
                "CREATE TABLE IF NOT EXISTS `lakesoul`.`default`.`sink_table` (\n" +
                        "    c0 STRING PRIMARY KEY NOT ENFORCED,\n" +
                        "    c1 STRING,\n" +
                        "    c2 STRING,\n" +
                        "    c3 STRING,\n" +
                        "    c4 STRING,\n" +
                        "    c5 STRING,\n" +
                        "    c6 STRING,\n" +
                        "    c7 STRING,\n" +
                        "    c8 STRING,\n" +
                        "    c9 STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'lakesoul',\n" +
                        "  'path' = 'file:///tmp/test_sink_table',\n" +
                        "  'hashBucketNum' = '1'\n" +
                        ")").await();
        tableEnvironment.executeSql(
                "CREATE TABLE sink_table2 (\n" +
                        "    c0 STRING,\n" +
                        "    c1 STRING,\n" +
                        "    c2 STRING,\n" +
                        "    c3 STRING,\n" +
                        "    c4 STRING,\n" +
                        "    c5 STRING,\n" +
                        "    c6 STRING,\n" +
                        "    c7 STRING,\n" +
                        "    c8 STRING,\n" +
                        "    c9 STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'blackhole'\n" +
                        ")").await();
        StreamStatementSet statementSet = (StreamStatementSet) tableEnvironment.createStatementSet();
        statementSet.addInsertSql("INSERT INTO `lakesoul`.`default`.`sink_table` SELECT * FROM source_table");
//        statementSet.addInsertSql("INSERT INTO sink_table2 SELECT * FROM source_table");
        statementSet.attachAsDataStream();
        env.execute();
    }
}