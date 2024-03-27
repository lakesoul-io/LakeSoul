package org.apache.flink.lakesoul.test;

import org.apache.flink.lakesoul.test.flinkSource.TestUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class SubstraitTest extends AbstractTestBase {

    private final String BATCH_TYPE = "batch";

    @Test
    public void testTimeStampLTZ() throws ExecutionException, InterruptedException {
        TableEnvironment insertEnv = TestUtils.createTableEnv(BATCH_TYPE);
        String createUserSql = "create table test_timestamp_ltz (" +
                "    createTime TIMESTAMP, " +
                "    modifyTime TIMESTAMP_LTZ " +
                ") WITH (" +
                "    'connector'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/test_timestamp_ltz") +
                "' )";

        insertEnv.executeSql("DROP TABLE if exists test_timestamp_ltz");
        insertEnv.executeSql(createUserSql);
        insertEnv.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        insertEnv.executeSql(
                        "INSERT INTO test_timestamp_ltz VALUES (TO_TIMESTAMP('1999-01-01 12:10:15'),TO_TIMESTAMP('1999-01-01 12:10:15'))")
                .await();

        TableEnvironment queryEnv = TestUtils.createTableEnv(BATCH_TYPE);
        queryEnv.getConfig().setLocalTimeZone(TimeZone.getTimeZone("America/Los_Angeles").toZoneId());

        List<Row> rows = CollectionUtil.iteratorToList(queryEnv.executeSql("SELECT " +
                "DATE_FORMAT(createTime, 'yyyy-MM-dd hh:mm:ss'), " +
                "DATE_FORMAT(modifyTime, 'yyyy-MM-dd hh:mm:ss')" +
                "FROM test_timestamp_ltz").collect());
        assertThat(rows.toString()).isEqualTo("[+I[1999-01-01 12:10:15, 1998-12-31 08:10:15]]");
    }

    @Test
    public void dateTypeTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = "select * from type_info where id=2 and name='Alice' and birthDay=TO_DATE('2023-05-10') and male=true and createTime=TO_TIMESTAMP('1995-10-10 13:10:20')";
//        String testSql = "select * from type_info " +
//                "where id = 2 " +
//                "and name = 'Alice' " +
//                "and birthDay = TO_DATE('2023-05-10')" +
//                "and male = true" +
//                "and level = '10.05'" +
//                "and zone = 'B' " +
//                "and height=1.90 " +
//                "and class= CAST(5 as TINYINT)" +
//                "and posn=CAST(2 as SMALLINT)" +
//                "and score=88 " +
//                "and createTime=TO_TIMESTAMP_LTZ(1612176000,0)";
        // Decimal? bytes? varbinary?
//        String testSql = "select * from type_info where  score= 88 and country=CAST( X'10912330' as VARBINARY)";
//        String testSql = "select * from type_info where createTime =TO_TIMESTAMP_LTZ(1612176000,0)";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[1, Bob, 1995-10-01, true, 10.01, A, 1.85, 3, 1,89, 100.11, [1, -81], [18, 67, 112, -105], 1990-01-07T10:10, 1995-10-01T07:10:00Z], " +
                        "+I[2, Alice, 2023-05-10, true, 10.05, B, 1.9, 5, 2,88, 500.31, [2, -1], [16, -111, 35, 48], 1995-10-10T13:10:20, 2021-02-01T10:40:00Z], " +
                        "+I[3, Jack, 2010-12-10, false, 10.12, D, 1.88, 9,3,67, 88.26, [1, -1], [-85, 18, -50, 9], 1999-01-01T12:10:15, 2000-10-01T07:15:00Z]]");

    }

    @Test
    public void funcTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
//        String testSql = "select * from type_info";
//        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
//        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
//        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
//        rows.sort(Comparator.comparing(Row::toString));
    }


    private void createLakeSoulSourceTableWithDateType(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table type_info (" +
                "    id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    birthDay DATE, " +
                "    male BOOLEAN, " +
                "    level VARCHAR, " +
                "    zone CHAR, " +
                "    height DOUBLE, " +
                "    class TINYINT, " +
                "    posn SMALLINT, " +
                "    score BIGINT, " +
                "    money DECIMAL(10,2), " +
                "    gapTime BYTES, " +
                "    country VARBINARY, " +
                "    createTime TIMESTAMP, " +
                "    modifyTime TIMESTAMP_LTZ " +
                ") WITH (" +
                "    'connector'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/tpye") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists type_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        tEnvs.executeSql("INSERT INTO type_info VALUES " +
                "(1, 'Bob', TO_DATE('1995-10-01'), true,'10.01','A',1.85,CAST(3 AS TINYINT),CAST(1 as SMALLINT),89,100.105,X'01AF',X'12437097',TO_TIMESTAMP('1990-01-07 10:10:00'),TO_TIMESTAMP('1995-10-01 15:10:00')), " +
                "(2, 'Alice', TO_DATE('2023-05-10'), true,'10.05','B',1.90,CAST(5 AS TINYINT),CAST(2 as SMALLINT),88,500.314,X'02FF',X'10912330',TO_TIMESTAMP('1995-10-10 13:10:20'),TO_TIMESTAMP_LTZ(1612176000,0)), " +
                "(3, 'Jack', TO_DATE('2010-12-10'),false,'10.12','D',1.88,CAST(9 AS TINYINT),CAST(3 as SMALLINT),67,88.262,X'01FF',X'AB12CE09',TO_TIMESTAMP('1999-01-01 12:10:15'),TO_TIMESTAMP('2000-10-01 15:15:00'))").await();

    }

}
