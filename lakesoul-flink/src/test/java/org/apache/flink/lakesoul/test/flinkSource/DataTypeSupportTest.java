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
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = String.format("select * from birth_info where id > 2");
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("America/Los_Angeles").toZoneId());
        TableImpl flinkTable = (TableImpl) tEnvs.sqlQuery(testSql);
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        TestUtils.checkEqualInAnyOrder(results, new String[]{"+I[3, Jack, 2010-12-10, false, 10.12, D, 1.88, 9, 67, 88.26, [1, -1], [-85, 18, -50, 9], 1999-01-01T12:10:15, 2000-10-01T15:15]"});
    }

    private void createLakeSoulSourceTableWithDateType(TableEnvironment tEnvs) throws ExecutionException, InterruptedException {
        String createUserSql = "create table birth_info (" +
                "    id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    birthDay DATE, " +
                "    male BOOLEAN, " +
                "    level VARCHAR, " +
                "    zone CHAR, " +
                "    height DOUBLE, " +
                "    class TINYINT, " +
                "    score BIGINT, " +
                "    money DECIMAL(10,2), " +
                "    gapTime BYTES, " +
                "    country VARBINARY, " +
                "    createTime TIMESTAMP, " +
                "    modifyTime TIMESTAMP_LTZ " +
                ") WITH (" +
                "    'format'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='/tmp/lakeSource/birth' )";
        tEnvs.executeSql("DROP TABLE if exists birth_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.executeSql("INSERT INTO birth_info VALUES " +
                "(1, 'Bob', TO_DATE('1995-10-01'), true,'10.01','A',1.85,3,89,100.105,X'01AF',X'12437097',TO_TIMESTAMP('1990-01-07 10:10:00'),TO_TIMESTAMP('1995-10-01 15:10:00')), " +
                "(2, 'Alice', TO_DATE('2023-05-10'), true,'10.05','B',1.90,5,88,500.314,X'02FF',X'10912330',TO_TIMESTAMP('1995-10-10 13:10:20'),TO_TIMESTAMP_LTZ(1612176000,0)), " +
                "(3, 'Jack', TO_DATE('2010-12-10'),false,'10.12','D',1.88,9,67,88.262,X'01FF',X'AB12CE09',TO_TIMESTAMP('1999-01-01 12:10:15'),TO_TIMESTAMP('2000-10-01 15:15:00'))").await();
    }
}
