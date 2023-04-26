package org.apache.flink.lakesoul.test.connector;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.lakesoul.connector.LakeSoulPartition;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.source.LakeSoulLookupTableSource;
import org.apache.flink.lakesoul.table.LakeSoulTableLookupFunction;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.filesystem.PartitionFetcher;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class LakeSoulLookupJoinCase {
    private static TableEnvironment tableEnv;
    private static LakeSoulCatalog lakeSoulCatalog;

    private static boolean ProbeTableGenerateDataOnce;

    @BeforeClass
    public static void setup() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        lakeSoulCatalog = new LakeSoulCatalog();
        tableEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        tableEnv.useCatalog(lakeSoulCatalog.getName());
        ProbeTableGenerateDataOnce = true;
        // create probe table
        if (ProbeTableGenerateDataOnce) {
            TestCollectionTableFactory.initData(
                    Arrays.asList(
                            Row.of(1, "a"),
                            Row.of(1, "c"),
                            Row.of(2, "b"),
                            Row.of(2, "c"),
                            Row.of(3, "c"),
                            Row.of(4, "d")));
        } else {
            TestCollectionTableFactory.initData(
                    Arrays.asList(
                            Row.of(1, "a"),
                            Row.of(1, "c"),
                            Row.of(2, "b"),
                            Row.of(2, "c"),
                            Row.of(3, "c"),
                            Row.of(4, "d"),

                            Row.of(1, "a"),
                            Row.of(1, "c"),
                            Row.of(2, "b"),
                            Row.of(2, "c"),
                            Row.of(3, "c"),
                            Row.of(4, "d")),
                    new ArrayList<>(),
                    5000);
        }

        tableEnv.executeSql(
                "create table default_catalog.default_database.probe (x int,y string, p as proctime()) "
                        + "with ('connector'='COLLECTION','is-bounded' = 'false')");

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // create the lakesoul non-partitioned non-hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_table (x int, y string, z int) with ('format'='','%s'='5min', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/bounded_table"));

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        // create the lakesoul non-partitioned hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_hash_table (x int, y string, z int, primary key(x) not enforced) with ('format'='','%s'='5min', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/bounded_hash_table"));

        // create the lakesoul partitioned non-hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_partition_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='','%s'='5min', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/bounded_partition_table"));

        // create the lakesoul partitioned hashed table
        tableEnv.executeSql(
                String.format(
                        "create table bounded_partition_hash_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string, primary key(x) not enforced) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='','%s'='5min', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/bounded_partition_hash_table"));

        // create the lakesoul partitioned table
        tableEnv.executeSql(
                String.format(
                        "create table partition_table (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='','%s'='5min', '%s'='true', '%s'='latest', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        "tmp/partition_tables"));
////        tableEnv.executeSql(
////                String.format(
////                        "create table partition_table (x int, y string, z int) partitioned by ("
////                                + " pt_year int, pt_mon string, pt_day string)"
////                                + " tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = 'partition-name', '%s'='2h')",
////                        STREAMING_SOURCE_ENABLE.key(),
////                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
////                        STREAMING_SOURCE_PARTITION_ORDER.key(),
////                        STREAMING_SOURCE_MONITOR_INTERVAL.key()));
//
        // create the hive partitioned table3 which uses default 'partition-name'.
//        tableEnv.executeSql(
//                String.format(
//                    "create table partition_table_1 (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
//                            + " pt_year, pt_mon, pt_day)"
//                            + " with ('format'='','%s'='5min', 'path'='%s')",
//                    JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/partition_table_1"));

        tableEnv.executeSql(
                String.format(
                        "create table partition_table_1 (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='', '%s'='5min', '%s' = 'true', '%s' = 'latest', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        "tmp/partition_table_1"));

        // create the lakesoul partitioned table which uses default 'partition-name' order and partition order keys are particular partition keys.
        tableEnv.executeSql(
                String.format(
                        "create table partition_table_2 (x int, y string, pt_year int, z string, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, z, pt_mon, pt_day)"
                                + " with ('format'='', '%s'='5min', '%s' = 'true', '%s' = 'latest', '%s'='pt_year,pt_mon,pt_day', '%s'='4', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        JobOptions.PARTITION_ORDER_KEYS.key(),
                        JobOptions.STREAMING_SOURCE_LATEST_PARTITION_NUMBER.key(),
                        "tmp/partition_table_2"));

        tableEnv.executeSql(
                String.format(
                        "create table partition_table_3 (x int, y string, z string, pt_year int, pt_mon string, pt_day string) partitioned by ("
                                + " pt_year, pt_mon, pt_day)"
                                + " with ('format'='', '%s'='30s', '%s' = 'true', '%s' = 'latest', 'path'='%s')",
                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(),
                        JobOptions.STREAMING_SOURCE_ENABLE.key(),
                        JobOptions.STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        "tmp/partition_table_3"));
//
//        // create the hive partitioned table3 which uses 'partition-time'.
//        tableEnv.executeSql(
//                String.format(
//                        "create table partition_table_2 (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
//                                + " pt_year, pt_mon, pt_day)"
//                                + " with ('format'='','%s'='5min', 'path'='%s')",
//                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/partition_table_2"));
////        tableEnv.executeSql(
////                String.format(
////                        "create table partition_table_2 (x int, y string, z int) partitioned by ("
////                                + " pt_year int, pt_mon string, pt_day string)"
////                                + " tblproperties ("
////                                + "'%s' = 'true',"
////                                + " '%s' = 'latest',"
////                                + " '%s' = '12h',"
////                                + " '%s' = 'partition-time', "
////                                + " '%s' = 'default',"
////                                + " '%s' = '$pt_year-$pt_mon-$pt_day 00:00:00')",
////                        STREAMING_SOURCE_ENABLE.key(),
////                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
////                        STREAMING_SOURCE_MONITOR_INTERVAL.key(),
////                        STREAMING_SOURCE_PARTITION_ORDER.key(),
////                        PARTITION_TIME_EXTRACTOR_KIND.key(),
////                        PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN.key()));
//
//        // create the hive partitioned table3 which uses 'create-time'.
//        tableEnv.executeSql(
//                String.format(
//                        "create table partition_table_3 (x int, y string, z int, pt_year int, pt_mon string, pt_day string) partitioned by ("
//                                + " pt_year, pt_mon, pt_day)"
//                                + " with ('format'='','%s'='5min', 'path'='%s')",
//                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key(), "tmp/partition_table_3"));
//        tableEnv.executeSql(
//                String.format(
//                        "create table partition_table_3 (x int, y string, z int) partitioned by ("
//                                + " pt_year int, pt_mon string, pt_day string)"
//                                + " tblproperties ("
//                                + " '%s' = 'true',"
//                                + " '%s' = 'latest',"
//                                + " '%s' = 'create-time')",
//                        STREAMING_SOURCE_ENABLE.key(),
//                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
//                        STREAMING_SOURCE_PARTITION_ORDER.key()));

        // create the hive table with columnar storage.
//        tableEnv.executeSql(
//                String.format(
//                        "create table columnar_table (x string) STORED AS PARQUET "
//                                + "tblproperties ('%s'='5min')",
//                        JobOptions.LOOKUP_JOIN_CACHE_TTL.key()));
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    }

    @Test
    public void testLookupOptions() throws Exception {
        LakeSoulTableLookupFunction lookupFunction1 =
                getLookupFunction("bounded_table");
//        FileSystemLookupFunction<LakeSoulPartition> lookupFunction2 =
//                getLookupFunction("partition_table");
        lookupFunction1.open(null);
//        lookupFunction2.open(null);

        // verify lookup cache TTL option is properly configured
        assertThat(lookupFunction1.getReloadInterval()).isEqualTo(Duration.ofMinutes(5));
//        assertThat(lookupFunction2.getReloadInterval()).isEqualTo(Duration.ofMinutes(120));
    }

    @Test
    public void testPartitionFetcherAndReader() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table values "
                                + "(1,'a',08,2019,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2020,'08','31'),"
                                + "(2,'b',22,2020,'08','31'),"
                                + "(3,'c',33,2020,'09','31')")
                .await();
        LakeSoulTableLookupFunction<LakeSoulPartition> lookupFunction =
                getLookupFunction("partition_table");
        lookupFunction.open(null);

        PartitionFetcher<LakeSoulPartition> fetcher = lookupFunction.getPartitionFetcher();
        PartitionFetcher.Context<LakeSoulPartition> context = lookupFunction.getFetcherContext();
        List<LakeSoulPartition> partitions = fetcher.fetch(context);
        // fetch latest partition by partition-name
        assertThat(partitions).hasSize(1);

        PartitionReader<LakeSoulPartition, RowData> reader = lookupFunction.getPartitionReader();
        reader.open(partitions);

        List<RowData> res = new ArrayList<>();
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(lakeSoulCatalog.getName(), "default", "partition_table");
        CatalogTable catalogTable =
                (CatalogTable) lakeSoulCatalog.getTable(tableIdentifier.toObjectPath());
        GenericRowData reuse =
                new GenericRowData(catalogTable.getUnresolvedSchema().getColumns().size());

        TypeSerializer<RowData> serializer =
                InternalSerializers.create(
                        DataTypes.ROW(
                                        catalogTable.getUnresolvedSchema().getColumns().stream()
                                                .map(FlinkUtil::getType)
                                                .toArray(DataType[]::new))
                                .getLogicalType());

        RowData row;
        while ((row = reader.read(reuse)) != null) {
            res.add(serializer.copy(row));
        }
        res.sort(Comparator.comparingInt(o -> o.getInt(0)));
        assertThat(res.toString()).isEqualTo("[+I(3,c,33,2020,09,31)]");
    }

    @Test
    public void testLookupJoinBoundedTable() throws Exception {
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert into bounded_table values (1,'a',10),(2,'a',21),(2,'b',22),(3,'c',33)")
                .await();
        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " default_catalog.default_database.probe as p "
                                        + " join bounded_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInExpectedOrder(results, "[+I[1, a, 10], +I[2, b, 22], +I[3, c, 33]]");
    }

    @Test
    public void testLookupJoinBoundedHashTable() throws Exception {
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert into bounded_hash_table values (1,'a',5),(2,'b',21),(2,'b',22),(1,'a',10),(3,'c',33)")
                .await();
        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z from "
                                        + " default_catalog.default_database.probe as p "
                                        + " join bounded_hash_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString()).isEqualTo("[+I[1, a, 10], +I[2, b, 22], +I[3, c, 33]]");
    }

    @Test
    public void testLookupJoinBoundedPartitionedTable() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
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
                                        + " default_catalog.default_database.probe as p"
                                        + " join bounded_partition_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        checkEqualInAnyOrder(results, new String[]{"+I[1, a, 8, 2019, 08, 01]", "+I[1, a, 10, 2020, 08, 31]", "+I[2, b, 22, 2020, 08, 31]"});
//        checkEqualInExpectedOrder(results, "[+I[1, a, 8, 2019, 08, 01], +I[1, a, 10, 2020, 08, 31], +I[2, b, 22, 2020, 08, 31]]");
    }

    @Test
    public void testLookupJoinBoundedPartitionedHashedTable() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
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
                                        + " default_catalog.default_database.probe as p"
                                        + " join bounded_partition_hash_table for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        for (Row row: results) System.out.println(row.toString());
        checkEqualInAnyOrder(results, new String[]{"+I[1, a, 8, 2019, 08, 01]", "+I[1, a, 10, 2020, 08, 31]", "+I[2, b, 22, 2020, 08, 31]"});
//        checkEqualInExpectedOrder(results, "[+I[1, a, 8, 2019, 08, 01], +I[1, a, 10, 2020, 08, 31], +I[2, b, 22, 2020, 08, 31]]");
    }

    @Test
    public void testLookupJoinPartitionedTableWithAllPartitionOrdered() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
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
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_1 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, a, 10, 2020, 09, 31], +I[2, b, 22, 2020, 09, 31], +I[3, c, 33, 2020, 09, 31]]");
    }

    @Test
    public void testLookupJoinPartitionedTableWithPartialPartitionOrdered() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
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
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_2 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, a, 10, 2020, 10, 31], +I[2, b, 22, 2020, 10, 31], +I[2, b, 50, 2020, 09, 31], +I[3, c, 33, 2020, 10, 31], +I[4, d, 50, 2020, 09, 31]]");
    }

    // Note: Please set ProbeTableGenerateDataOnce to "false" before run this test
    @Test
    public void testLookupJoinPartitionedTableWithCacheReloaded() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_3 values "
                                + "(1,'a','11',2019,'09','01'),"
                                + "(1,'a','10',2020,'10','31'),"
                                + "(2,'b','50',2020,'09','31'),"
                                + "(2,'a','53',2020,'09','31'),"
                                + "(3,'c','33',2020,'10','31'),"
                                + "(4,'d','50',2020,'09','31'),"
                                + "(2,'b','22',2020,'10','31'),"
                                + "(3,'d','10',2020,'10','31'),"
                                + "(1,'a','101',2020,'08','01'),"
                                + "(2,'a','121',2020,'08','01'),"
                                + "(2,'b','122',2020,'08','01')")
                .await();
        Thread insertDimTableThread = new InsertDataThread(10000,
                "insert overwrite partition_table_3 values "
                + "(1,'a','88',2021,'05','01'),"
                + "(1,'a','10',2021,'05','01'),"
                + "(2,'b','50',2021,'05','01'),"
                + "(3,'c','99',2021,'05','01'),"
                + "(2,'b','66',2020,'09','31')");
        insertDimTableThread.start();
        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_3 for system_time as of p.p as b on p.x=b.x and p.y=b.y");


        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        insertDimTableThread.join();
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, a, 10, 2020, 10, 31], +I[2, b, 22, 2020, 10, 31], " +
                                "+I[3, c, 33, 2020, 10, 31], +I[1, a, 88, 2021, 05, 01], +I[1, a, 10, 2021, 05, 01], " +
                                "+I[2, b, 50, 2021, 05, 01], +I[3, c, 99, 2021, 05, 01]]"
                );
    }

    @Test
    public void testLookupJoinPartitionedTableWithPartitionTime() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_2 values "
                                + "(1,'a',08,2020,'08','01'),"
                                + "(1,'a',10,2020,'08','31'),"
                                + "(2,'a',21,2019,'08','31'),"
                                + "(2,'b',22,2020,'08','31'),"
                                + "(3,'c',33,2017,'08','31'),"
                                + "(1,'a',101,2017,'09','01'),"
                                + "(2,'a',121,2019,'09','01'),"
                                + "(2,'b',122,2019,'09','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_2 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString())
                .isEqualTo("[+I[1, a, 10, 2020, 08, 31], +I[2, b, 22, 2020, 08, 31]]");
    }

    @Test
    public void testLookupJoinPartitionedTableWithCreateTime() throws Exception {
        // constructs test data using dynamic partition
        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite partition_table_3 values "
                                + "(1,'a',08,2020,'month1','01'),"
                                + "(1,'a',10,2020,'month2','02'),"
                                + "(2,'a',21,2020,'month1','02'),"
                                + "(2,'b',22,2020,'month3','20'),"
                                + "(3,'c',22,2020,'month3','20'),"
                                + "(3,'c',33,2017,'08','31'),"
                                + "(1,'a',101,2017,'09','01'),"
                                + "(2,'a',121,2019,'09','01'),"
                                + "(2,'b',122,2019,'09','01')")
                .await();

        // inert a new partition
        batchEnv.executeSql(
                        "insert overwrite partition_table_3 values "
                                + "(1,'a',101,2020,'08','01'),"
                                + "(2,'a',121,2020,'08','01'),"
                                + "(2,'b',122,2020,'08','01')")
                .await();

        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select p.x, p.y, b.z, b.pt_year, b.pt_mon, b.pt_day from "
                                        + " default_catalog.default_database.probe as p"
                                        + " join partition_table_3 for system_time as of p.p as b on p.x=b.x and p.y=b.y");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString())
                .isEqualTo("[+I[1, a, 101, 2020, 08, 01], +I[2, b, 122, 2020, 08, 01]]");
    }

    @Test
    public void testLookupJoinWithLookUpSourceProjectPushDown() throws Exception {

        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
        batchEnv.useCatalog(lakeSoulCatalog.getName());
        batchEnv.executeSql(
                        "insert overwrite bounded_table values (1,'a',10),(2,'b',22),(3,'c',33)")
                .await();
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        TableImpl flinkTable =
                (TableImpl)
                        tableEnv.sqlQuery(
                                "select b.x, b.z from "
                                        + " default_catalog.default_database.probe as p "
                                        + " join bounded_table for system_time as of p.p as b on p.x=b.x");
        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
        assertThat(results.toString())
                .isEqualTo("[+I[1, 10], +I[1, 10], +I[2, 22], +I[2, 22], +I[3, 33]]");
    }

//    @Test
//    public void testLookupJoinTableWithColumnarStorage() throws Exception {
//        // constructs test data, as the DEFAULT_SIZE of VectorizedColumnBatch is 2048, we should
//        // write as least 2048 records to the test table.
//        List<Row> testData = new ArrayList<>(4096);
//        for (int i = 0; i < 4096; i++) {
//            testData.add(Row.of(String.valueOf(i)));
//        }
//
//        // constructs test data using values table
//        TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
//        batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
//        batchEnv.useCatalog(lakeSoulCatalog.getName());
//        String dataId = TestValuesTableFactory.registerData(testData);
//        batchEnv.executeSql(
//                String.format(
//                        "create table value_source(x string, p as proctime()) with ("
//                                + "'connector' = 'values', 'data-id' = '%s', 'bounded'='true')",
//                        dataId));
//        batchEnv.executeSql("insert overwrite columnar_table select x from value_source").await();
//        TableImpl flinkTable =
//                (TableImpl)
//                        tableEnv.sqlQuery(
//                                "select t.x as x1, c.x as x2 from value_source t "
//                                        + "left join columnar_table for system_time as of t.p c "
//                                        + "on t.x = c.x where c.x is null");
//        List<Row> results = CollectionUtil.iteratorToList(flinkTable.execute().collect());
//        assertThat(results)
//                .as(
//                        "All records should be able to be joined, and the final results should be empty.")
//                .isEmpty();
//    }

    private LakeSoulTableLookupFunction getLookupFunction(String tableName)
            throws Exception {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(lakeSoulCatalog.getName(), "default", tableName);
        CatalogTable catalogTable =
                (CatalogTable) lakeSoulCatalog.getTable(tableIdentifier.toObjectPath());
        LakeSoulLookupTableSource lakeSoulLookupTableSource =
                (LakeSoulLookupTableSource)
                        FactoryUtil.createTableSource(
                                lakeSoulCatalog,
                                tableIdentifier,
                                tableEnvInternal
                                        .getCatalogManager()
                                        .resolveCatalogTable(catalogTable),
                                tableEnv.getConfig().getConfiguration(),
                                Thread.currentThread().getContextClassLoader(),
                                false);
        LakeSoulTableLookupFunction lookupFunction =
                (LakeSoulTableLookupFunction)
                        lakeSoulLookupTableSource.getLookupFunction(new int[][] {{0}});
        return lookupFunction;
    }

    // check whether every row in result is included in expectedResultSet, no requirement for the order of rows
    private void checkEqualInAnyOrder(List<Row> results, String[] expectedResult) {
        assertThat(results.stream().map(row -> row.toString()).collect(Collectors.toList())).
                containsExactlyInAnyOrder(expectedResult);
    }

    // checkout whether rows in results are returned in expected order
    private void checkEqualInExpectedOrder(List<Row> results, String expectedString) {
        assertThat(results.toString())
                .isEqualTo(expectedString);
    }

    @AfterClass
    public static void tearDown() {
        tableEnv.executeSql("drop table if exists bounded_table");
        tableEnv.executeSql("drop table if exists bounded_hash_table");
        tableEnv.executeSql("drop table if exists bounded_partition_table");
        tableEnv.executeSql("drop table if exists bounded_partition_hash_table");
        tableEnv.executeSql("drop table if exists partition_table");
        tableEnv.executeSql("drop table if exists partition_table_1");
        tableEnv.executeSql("drop table if exists partition_table_2");
        tableEnv.executeSql("drop table if exists partition_table_3");

        if (lakeSoulCatalog != null) {
            lakeSoulCatalog.close();
        }
    }

    private class InsertDataThread extends Thread { ;

        private int sleepMilliSeconds;

        private String insertSql;
        public InsertDataThread(int sleepMilliSeconds, String insertSql) {
            this.sleepMilliSeconds = sleepMilliSeconds;
            this.insertSql = insertSql;
        }

        @Override
        public void run() {
            TableEnvironment batchEnv = FlinkUtil.createTableEnvInBatchMode(SqlDialect.DEFAULT);
            batchEnv.registerCatalog(lakeSoulCatalog.getName(), lakeSoulCatalog);
            batchEnv.useCatalog(lakeSoulCatalog.getName());
            try {
                Thread.sleep(sleepMilliSeconds);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                batchEnv.executeSql(insertSql).await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
