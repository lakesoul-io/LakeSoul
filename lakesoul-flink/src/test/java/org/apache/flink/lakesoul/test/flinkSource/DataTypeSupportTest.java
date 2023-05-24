package org.apache.flink.lakesoul.test.flinkSource;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

public class DataTypeSupportTest {
    private String BATCH_TYPE = "batch";

    @Test
    public void testLakesoulSourceWithDateType() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv();
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = String.format("select * from birth_info where id > 2");
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("America/Los_Angeles").toZoneId());
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSql);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 2010-12-10, 10.12, 1.88, 9, 67, 88.26, 1999-01-01T00:00, 2000-10-01T15:15]"});
    }

    private void createLakeSoulSourceTableWithDateType(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table birth_info (" +
                "    id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    birthDay DATE, " +
                "    level VARCHAR, " +
                "    height DOUBLE, " +
                "    class TINYINT, " +
                "    score BIGINT, " +
                "    money DECIMAL(10,2), " +
                "    createTime TIMESTAMP, " +
                "    modifyTime TIMESTAMP WITHOUT TIME ZONE " +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/birth' )";
        tEnvs.executeSql("DROP TABLE if exists birth_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO birth_info VALUES " +
                "(1, 'Bob', TO_DATE('1995-10-01'), '10.01',1.85,3,89,100.105,TO_TIMESTAMP('1990-01-07'),TO_TIMESTAMP('1995-10-01 15:10:00')), " +
                "(2, 'Alice', TO_DATE('2023-05-10'), '10.05',1.90,5,88,500.314,TO_TIMESTAMP('1995-10-10'),TO_TIMESTAMP_LTZ(1612176000,0)), " +
                "(3, 'Jack', TO_DATE('2010-12-10'), '10.12',1.88,9,67,88.262,TO_TIMESTAMP('1999-01-01'),TO_TIMESTAMP('2000-10-01 15:15:00'))").await();
    }
}
