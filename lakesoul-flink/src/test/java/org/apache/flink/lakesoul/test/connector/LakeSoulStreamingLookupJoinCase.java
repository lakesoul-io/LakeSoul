/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.test.connector;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.List;

import static org.apache.flink.lakesoul.test.AbstractTestBase.getTempDirUri;

public class LakeSoulStreamingLookupJoinCase {

    private static TableEnvironment tableEnv;
    private static LakeSoulCatalog lakeSoulCatalog;

    private static TableEnvironment batchEnv;

    @BeforeClass
    public static void setup() throws Exception {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        lakeSoulCatalog = new LakeSoulCatalog();
        tableEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tableEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());

        tableEnv.executeSql("drop table if exists probe");
        tableEnv.executeSql("drop table if exists partition_table_3");
        tableEnv.executeSql("drop table if exists bounded_table");
        tableEnv.executeSql("drop table if exists bounded_hash_table");
        tableEnv.executeSql("drop table if exists bounded_partition_table");
        tableEnv.executeSql("drop table if exists bounded_partition_hash_table");
        tableEnv.executeSql("drop table if exists partition_table_1");
        tableEnv.executeSql("drop table if exists partition_table_2");

        tableEnv.executeSql(
                "create table if not exists probe (x int,y string" +
//                        ", p as proctime()" +
                        ") "
                        + String.format("with ('format'='lakesoul', 'path'='%s')",
                        getTempDirUri("/probe")));

        batchEnv.executeSql(
                        "insert overwrite probe values "
                                + "(1,'a'),"
                                + "(1,'c'),"
                                + "(2,'b'),"
                                + "(2,'c'),"
                                + "(3,'c'),"
                                + "(4,'d')")
                .await();

//        tableEnv.executeSql(
//                String.format(
//                        "create table if not exists partition_table_3 (x int, y string, z string, pt_year int, pt_mon string, pt_day string) partitioned by ("
//                                + " pt_year, pt_mon, pt_day)"
//                                + " with ('format'='lakesoul', '%s'='30s', '%s' = 'true', '%s' = 'latest', '%s'='false', 'path'='%s')",
//                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
//                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
//                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
//                        LakeSoulSinkOptions.USE_CDC.key(),
//                        "tmp/partition_table_3"));
    }

    @AfterClass
    public static void tearDown() {
        tableEnv.executeSql("drop table if exists probe");
        tableEnv.executeSql("drop table if exists bounded_table");
        tableEnv.executeSql("drop table if exists partition_table_3");

        if (lakeSoulCatalog != null) {
            lakeSoulCatalog.close();
        }
    }

    //    @Test
    public void testLookupJoinBoundedTable() throws Exception {
        // create the lakesoul non-partitioned non-hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_table (x int, y string, z int) with ('format'='lakesoul','%s'='5min', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/bounded_table")));

        batchEnv.executeSql(
                        "insert into bounded_table values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join bounded_table " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }

    //    @Test
    public void testLookupJoinBoundedHashTable() throws Exception {
        // create the lakesoul non-partitioned hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_hash_table (x int, y string, z int, primary key(x) not enforced) with ('format'='lakesoul','%s'='5min', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/bounded_hash_table")));

        batchEnv.executeSql(
                        "insert into bounded_hash_table values (1,'a',5),(2,'b',21),(2,'b',22),(1,'a',10),(3,'c',33)")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join bounded_hash_table " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }

    //    @Test
    public void testLookupJoinBoundedPartitionedTable() throws Exception {
        // create the lakesoul partitioned non-hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_partition_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='lakesoul','%s'='5min', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/bounded_partition_table")));

        batchEnv.executeSql(
                        "insert overwrite bounded_partition_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31'),"
                                + "(2,'b',22,2020,'08','31')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join bounded_partition_table " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }

    //    @Test
    public void testLookupJoinBoundedPartitionedHashedTable() throws Exception {
        // create the lakesoul partitioned hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_partition_hash_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string, primary key(x) not enforced) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='lakesoul','%s'='5min', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/bounded_partition_hash_table")));

        batchEnv.executeSql(
                        "insert overwrite bounded_partition_hash_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31'),"
                                + "(2,'b',21,2020,'08','31'),"
                                + "(2,'b',22,2020,'08','31')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join bounded_partition_hash_table " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }

    //    @Test
    public void testLookupJoinPartitionedTableWithAllPartitionOrdered() throws Exception {
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_1 (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                +
                                " with ('format'='lakesoul', '%s'='5min', '%s' = 'true', '%s' = 'latest', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/partition_table_1")));

        batchEnv.executeSql(
                        "insert overwrite partition_table_1 values "
                                + "(1,'a',08,2019,'09','01'),"
                                + "(1,'a',10,2020,'09','31'),"
                                + "(2,'a',21,2020,'09','31'),"
                                + "(2,'b',22,2020,'09','31'),"
                                + "(3,'c',33,2020,'09','31'),"
                                + "(1,'a',101,2020,'08','01'),"
                                + "(2,'a',121,2020,'08','01'),"
                                + "(2,'b',122,2020,'08','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join partition_table_1 " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }

//    @Test
//    public void test() throws Exception {
//        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
//        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
//        batchEnv.useCatalog(lakeSoulCatalog.getName());
////        batchEnv.executeSql("select * from probe").print();
//        Thread insertDimTableThread = new LakeSoulLookupJoinCase.InsertDataThread(5000,
//                "insert overwrite partition_table_3 values "
//                        + "(1,'a','88',2021,'05','01'),"
//                        + "(1,'a','10',2021,'05','01'),"
//                        + "(2,'b','50',2021,'05','01'),"
//                        + "(3,'c','99',2021,'05','01'),"
//                        + "(2,'b','66',2020,'09','31')");
//
//        insertDimTableThread.start();
//
//        Thread insertProbeThread = new LakeSoulLookupJoinCase.InsertDataThread(10000,
//                "insert overwrite probe values "
//                        + "(1,'a'),"
//                        + "(1,'c'),"
//                        + "(2,'b'),"
//                        + "(2,'c'),"
//                        + "(3,'c'),"
//                        + "(4,'d')");
//
//
//
//        insertProbeThread.start();
//        TableImpl flinkTable =
//                (TableImpl)
//                        tableEnv.sqlQuery(
//                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
//                                        + " (select " +
//                                        "   *" +
//                                        "   , PROCTIME() as proctime " +
//                                        "from probe) as p"
//                                        + " join partition_table_3 " +
//                                        "for system_time as of p.proctime " +
//                                        "as b on p.x=b.x and p.y=b.y");
//
//        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
////        tableEnv.executeSql("select * from probe").print();
//        insertDimTableThread.join();
//        insertProbeThread.join();
//    }

    //    @Test
    public void testLookupJoinPartitionedTableWithPartialPartitionOrdered() throws Exception {
        // create the lakesoul partitioned table which uses default 'partition-name' order and partition order keys are particular partition keys.
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_2 (x int, y string, pt_year int, z string, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, z, pt_mon, pt_day)"
                                +
                                " with ('format'='lakesoul', '%s'='5min', '%s' = 'true', '%s' = 'latest', '%s'='pt_year,pt_mon,pt_day', '%s'='4', '%s'='false', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        JobOptions.PARTITION_ORDER_KEYS.key(),
                        JobOptions.STREAMING_SOURCE_LATEST_PARTITION_NUMBER.key(),
                        LakeSoulSinkOptions.USE_CDC.key(),
                        getTempDirUri("/partition_table_2")));

        batchEnv.executeSql(
                        "insert overwrite partition_table_2 values "
                                + "(1,'a',2019,'11','09','01'),"
                                + "(1,'a',2020,'10','10','31'),"
                                + "(2,'b',2020,'50','09','31'),"
                                + "(2,'a',2020,'53','09','31'),"
                                + "(3,'c',2020,'33','10','31'),"
                                + "(4,'d',2020,'50','09','31'),"
                                + "(2,'b',2020,'22','10','31'),"
                                + "(3,'d',2020,'10','10','31'),"
                                + "(1,'a',2020,'101','08','01'),"
                                + "(2,'a',2020,'121','08','01'),"
                                + "(2,'b',2020,'122','08','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " (select " +
                                        "   *" +
                                        "   , PROCTIME() as proctime " +
                                        "from probe) as p"
                                        + " join partition_table_2 " +
                                        "for system_time as of p.proctime " +
                                        "as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
    }
}
