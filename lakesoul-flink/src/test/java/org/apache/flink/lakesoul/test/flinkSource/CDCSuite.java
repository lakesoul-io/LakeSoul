package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class CDCSuite {
    private String BATCH_TYPE = "batch";
    private String STREAMING_TYPE = "streaming";

    @Test
    public void testLakeSoulSourceCdc() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceCdcTable(createTableEnv);
        String createSql = "create table user_sink (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    op VARCHAR" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'use_cdc'='true'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/cdc_sink' )";

        TableEnvironment createCDCTableEnv = TestUtils.createTableEnv();
        createCDCTableEnv.executeSql("DROP TABLE if exists user_sink");
        createCDCTableEnv.executeSql(createSql);
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String cdcSql = "insert into user_sink select * from user_cdc";
        tEnvs.executeSql(cdcSql);
        String selectSql = "select * from user_sink";
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(selectSql);
        flinkTable.execute().print();
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[1, Bob, delete]", "+I[3, Mary, insert]", "+I[2, Alice, update]"});
    }

    @Test
    public void testLakeSoulSourceStreamCdc() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceCdcTable(createTableEnv);
        String createSql = "create table user_sink (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    op VARCHAR" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'use_cdc'='true'," +
                "    'path'='/tmp/lakeSource/cdc_sink' )";
        TableEnvironment createCDCTableEnv = TestUtils.createTableEnv();
        createCDCTableEnv.executeSql("DROP TABLE if exists user_sink");
        createCDCTableEnv.executeSql(createSql);
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        String cdcSql = "insert into user_sink select * from user_cdc";
        tEnvs.executeSql(cdcSql).await();
        String selectSql = "select * from user_sink";
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(selectSql);
        flinkTable.execute().print();
//        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
//        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[1, Bob, delete]", "+I[3, Mary, insert]", "+I[2, Alice, update]"});
    }

    private void createLakeSoulSourceCdcTable(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createSql = "create table user_cdc (" +
                "    order_id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    op VARCHAR" +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'use_cdc'='true'," +
                "    'path'='/tmp/lakeSource/cdc' )";
        tEnvs.executeSql("DROP TABLE if exists user_cdc");
        tEnvs.executeSql(createSql);
        tEnvs.executeSql("INSERT INTO user_cdc VALUES (1, 'Bob', 'insert'), (2, 'Alice', 'insert'), (1, 'Bob', 'delete')").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_cdc VALUES (3, 'Mary', 'insert')").await();
        Thread.sleep(1000l);
        tEnvs.executeSql("INSERT INTO user_cdc VALUES (2, 'Alice', 'update')").await();
    }
}
