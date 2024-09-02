package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import io.substrait.extension.SimpleExtension;
import org.apache.flink.lakesoul.test.flinkSource.TestUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

public class SubstraitTest extends AbstractTestBase {

    public static void main(String[] args) {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createTableEnv.executeSql("select * from nation where n_regionkey = 0 or n_nationkey > 14").print();
    }

    @Test
    public void loadSubStrait() throws IOException {
        SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
    }

    @Test
    public void dateTypeTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        // TIMESTAMP_LTZ, BYTES and VARBINARY is not supported at present
        String testSql = "select * from type_info " +
                "where id=2 " +
                "and name='Alice' " +
                "and birthday=TO_DATE('2023-05-10') " +
                "and male=true " +
                "and level='10.05' " +
                "and zone='B' " +
                "and height=1.90 " +
                "and score=88 " +
                "and money=500.31 " +
                "and createTime=TO_TIMESTAMP('1995-10-10 13:10:20') ";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[2, Alice, 2023-05-10, true, 10.05, B, 1.9, 5, 2, 88, 500.31, [2, -1], [16, -111, 35, 48], 1995-10-10T13:10:20, 2021-02-01T10:40:00Z]]");

    }

    @Test
    public void nullTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = "select * from type_info where id IS NULL and male IS NOT NULL";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[null, Jack, 2010-12-10, false, 10.12, D, 1.88, 9, 3, 67, 88.26, [1, -1], [-85, 18, -50, 9], 1999-01-01T12:10:15, 2000-10-01T07:15:00Z]]");
    }

    @Test
    public void logicTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = "select * from type_info where id=1 or id=2 and not name='Alice'";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[1, Bob, 1995-10-01, true, 10.01, A, 1.85, 3, 1, 89, 100.11, [1, -81], [18, 67, 112, -105], 1990-01-07T10:10, 1995-10-01T07:10:00Z]]");
    }

    @Test
    public void partitionTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            sb.append("0");
        }
        String testSql = String.format("select * from type_info where zone='%s' or zone='A'", sb.toString());
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[1, Bob, 1995-10-01, true, 10.01, A, 1.85, 3, 1, 89, 100.11, [1, -81], [18, 67, 112, -105], 1990-01-07T10:10, 1995-10-01T07:10:00Z]]");
    }

    @Test
    public void cmpTest() throws ExecutionException, InterruptedException {
        TableEnvironment createTableEnv = TestUtils.createTableEnv(BATCH_TYPE);
        createLakeSoulSourceTableWithDateType(createTableEnv);
        String testSql = "select * from type_info where id=1 and name<>'Alice' and height<CAST(1.90 as DOUBLE) and score >= CAST(89 as BIGINT)";
        StreamTableEnvironment tEnvs = TestUtils.createStreamTableEnv(BATCH_TYPE);
        List<Row> rows = CollectionUtil.iteratorToList(tEnvs.executeSql(testSql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        assertThat(rows.toString()).isEqualTo(
                "[+I[1, Bob, 1995-10-01, true, 10.01, A, 1.85, 3, 1, 89, 100.11, [1, -81], [18, 67, 112, -105], 1990-01-07T10:10, 1995-10-01T07:10:00Z]]");
    }


    private void createLakeSoulSourceTableWithDateType(TableEnvironment tEnvs)
            throws ExecutionException, InterruptedException {
        String createUserSql = "create table type_info (" +
                "    id INT," +
                "    name STRING PRIMARY KEY NOT ENFORCED," +
                "    birthday DATE, " +
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
                ") " +
                "PARTITIONED BY (`zone`,`country`,`money`)" +

                "WITH (" +
                "    'connector'='lakesoul'," +
                "    'hashBucketNum'='2'," +
                "    'path'='" + getTempDirUri("/lakeSource/type") +
                "' )";
        tEnvs.executeSql("DROP TABLE if exists type_info");
        tEnvs.executeSql(createUserSql);
        tEnvs.getConfig().setLocalTimeZone(TimeZone.getTimeZone("Asia/Shanghai").toZoneId());
        tEnvs.executeSql("INSERT INTO type_info VALUES " +
                "(1, 'Bob', TO_DATE('1995-10-01'), true, '10.01', 'A', 1.85, CAST(3 AS TINYINT), CAST(1 as SMALLINT), 89, 100.105, X'01AF', X'12437097', TO_TIMESTAMP('1990-01-07 10:10:00'), TO_TIMESTAMP('1995-10-01 15:10:00')), " +
                "(2, 'Alice', TO_DATE('2023-05-10'), true, '10.05', 'B', 1.90, CAST(5 AS TINYINT), CAST(2 as SMALLINT), 88, 500.314, X'02FF', X'10912330', TO_TIMESTAMP('1995-10-10 13:10:20'), TO_TIMESTAMP_LTZ(1612176000, 0)), " +
                "(CAST(null as INT), 'Jack', TO_DATE('2010-12-10'), false, '10.12', 'D', 1.88,CAST(9 AS TINYINT), CAST(3 as SMALLINT), 67, 88.262, X'01FF', X'AB12CE09', TO_TIMESTAMP('1999-01-01 12:10:15'), TO_TIMESTAMP('2000-10-01 15:15:00'))").await();
    }
}
