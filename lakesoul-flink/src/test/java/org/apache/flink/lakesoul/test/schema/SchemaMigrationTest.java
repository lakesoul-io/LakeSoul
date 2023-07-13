package org.apache.flink.lakesoul.test.schema;

import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CATALOG_PATH;
import static org.assertj.core.api.Assertions.assertThat;

public class SchemaMigrationTest extends AbstractTestBase {

    private static CreateTableAtSinkCatalog testCatalog;

    private static LakeSoulCatalog validateCatalog;

    private static TableEnvironment testEnv, validateEnv;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @BeforeClass
    public static void setup() {
        testCatalog = new CreateTableAtSinkCatalog();
        testEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        LakeSoulTestUtils.registerLakeSoulCatalog(testEnv, testCatalog);

        validateCatalog = new LakeSoulCatalog();
        validateEnv = LakeSoulTestUtils.createTableEnvInBatchMode();
        LakeSoulTestUtils.registerLakeSoulCatalog(validateEnv, validateCatalog);
    }

    @Test
    public void testFromIntToBigInt() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new IntType());
        RowType.RowField afterField = new RowType.RowField("a", new BigIntType());

        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1), (2)",
                "insert into test_sink values (300000000000), (4000000000000)",
                "[+I[a, INT, true, null, null, null]]",
                "[+I[a, BIGINT, true, null, null, null]]",
                "[+I[1], +I[2]]",
                "[+I[1], +I[2], +I[300000000000], +I[4000000000000]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testFromIntToBigIntWithoutAllowance() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new IntType());
        RowType.RowField afterField = new RowType.RowField("a", new BigIntType());

        System.setProperty("datatype.cast.allow_precision_inc", "false");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1), (2)",
                "insert into test_sink values (300000000000), (4000000000000)",
                "[+I[a, INT, true, null, null, null]]",
                "[+I[a, BIGINT, true, null, null, null]]",
                "[+I[1], +I[2]]",
                "[+I[1], +I[2], +I[300000000000], +I[4000000000000]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testFromBigIntToInt() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new BigIntType());
        RowType.RowField afterField = new RowType.RowField("a", new IntType());

        System.clearProperty("datatype.cast.allow_precision_loss");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (10000000000), (20000000000)",
                "insert into test_sink values (3), (4)",
                "[+I[a, BIGINT, true, null, null, null]]",
                "",
                "[+I[10000000000], +I[20000000000]]",
                ""
        );
    }

    @Test
    public void testFromBigIntToTinyIntWithAllowance() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new BigIntType());
        RowType.RowField afterField = new RowType.RowField("a", new TinyIntType());

        System.setProperty("datatype.cast.allow_precision_loss", "true");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (10000000000), (20000000000)",
                "insert into test_sink values (CAST(3 as TINYINT)), (CAST(4 as TINYINT))",
                "[+I[a, BIGINT, true, null, null, null]]",
                "[+I[a, TINYINT, true, null, null, null]]",
                "[+I[10000000000], +I[20000000000]]",
                "[+I[null], +I[null], +I[3], +I[4]]"
        );
    }

    @Test
    public void testFromFloatToDouble() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new FloatType());
        RowType.RowField afterField = new RowType.RowField("a", new DoubleType());

        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111), (2.2222222222)",
                "insert into test_sink values (3.33333333333), (4.44444444444)",
                "[+I[a, FLOAT, true, null, null, null]]",
                "[+I[a, DOUBLE, true, null, null, null]]",
                "[+I[1.1111112], +I[2.2222223]]",
                "[+I[1.1111111640930176], +I[2.222222328186035], +I[3.33333333333], +I[4.44444444444]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testFromFloatToDoubleWithoutAllowance() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new FloatType());
        RowType.RowField afterField = new RowType.RowField("a", new DoubleType());

        System.setProperty("datatype.cast.allow_precision_inc", "false");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111), (2.2222222222)",
                "insert into test_sink values (3.33333333333), (4.44444444444)",
                "[+I[a, FLOAT, true, null, null, null]]",
                "[+I[a, DOUBLE, true, null, null, null]]",
                "[+I[1.1111112], +I[2.2222223]]",
                "[+I[1.1111111640930176], +I[2.222222328186035], +I[3.33333333333], +I[4.44444444444]]"
        );
    }

    @Test
    public void testAddColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldB = new RowType.RowField("b", new IntType());

        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeField.getName(), beforeField.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterFieldA.getName(), afterFieldA.getType().asSerializableString())
                                .column(afterFieldB.getName(), afterFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111), (2.2222222222)",
                "insert into test_sink values (3.33333333333, 33), (4.4444444444, 44)",
                "[+I[a, FLOAT, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[1.1111112], +I[2.2222223]]",
                "[+I[1.1111112, null], +I[2.2222223, null], +I[3.3333333, 33], +I[4.4444447, 44]]"
        );
    }

    @Test
    public void testDropColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType());
        RowType.RowField afterField = new RowType.RowField("a", new FloatType());

        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterField.getName(), afterField.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111, 1), (2.2222222222, 2)",
                "insert into test_sink values (3.33333333333), (4.4444444444)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null]]",
                "[+I[1.1111112, 1], +I[2.2222223, 2]]",
                "[+I[1.1111112], +I[2.2222223], +I[3.3333333], +I[4.4444447]]"
        );
    }

    @Test
    public void testDropColumnLogically() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(LakeSoulSinkOptions.CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        options.put(LakeSoulSinkOptions.LOGICALLY_DROP_COLUM.key(), "true");
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType());
        RowType.RowField afterField = new RowType.RowField("a", new FloatType());

        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterField.getName(), afterField.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111, 1), (2.2222222222, 2)",
                "insert into test_sink values (3.33333333333), (4.4444444444)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[1.1111112, 1], +I[2.2222223, 2]]",
                "[+I[1.1111112, 1], +I[2.2222223, 2], +I[3.3333333, null], +I[4.4444447, null]]"
        );
        Object properties = new DBManager().getTableInfoByName("test_sink").getProperties().get(DBConfig.TableInfoProperty.DROPPED_COLUMN);
        assertThat(Objects.requireNonNull(properties).toString()).contains("b");
    }

    @Test(expected = ExecutionException.class)
    public void testFromDoubleToFloat() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new DoubleType());
        RowType.RowField afterField = new RowType.RowField("a", new FloatType());

        System.clearProperty("datatype.cast.allow_precision_loss");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1.11111111111), (2.22222222222)",
                "insert into test_sink values (3.3333333333), (4.44444444444)",
                "[+I[a, DOUBLE, true, null, null, null]]",
                "",
                "[+I[1.11111111111], +I[2.22222222222]]",
                ""
        );
    }

    @Test
    public void testFromDoubleToFloatWithAllowance() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new DoubleType());
        RowType.RowField afterField = new RowType.RowField("a", new FloatType());

        System.setProperty("datatype.cast.allow_precision_loss", "true");
        testSchemaMigration(
                CatalogTable.of(Schema.newBuilder().column(beforeField.getName(), beforeField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                CatalogTable.of(Schema.newBuilder().column(afterField.getName(), afterField.getType().asSerializableString()).build(), "", Collections.emptyList(), options),
                "insert into test_sink values (1.11111111111), (2.22222222222)",
                "insert into test_sink values (3.3333333333), (4.44444444444)",
                "[+I[a, DOUBLE, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null]]",
                "[+I[1.11111111111], +I[2.22222222222]]",
                "[+I[1.1111112], +I[2.2222223], +I[3.3333333], +I[4.4444447]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testChangeDatatypeOfPartitionColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType());
        RowType.RowField afterFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldB = new RowType.RowField("b", new BigIntType());

        System.clearProperty("datatype.cast.allow_precision_loss");
        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.singletonList("b"), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterFieldA.getName(), afterFieldA.getType().asSerializableString())
                                .column(afterFieldB.getName(), afterFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.singletonList("b"), options),
                "insert into test_sink values (1.1111111111, 11), (2.2222222222, 22)",
                "insert into test_sink values (3.33333333333, 33), (4.4444444444, 44)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, BIGINT, true, null, null, null]]",
                "[+I[1.1111112, 11], +I[2.2222223, 22]]",
                "[+I[1.1111112, null], +I[2.2222223, null], +I[3.3333333, 33], +I[4.4444447, 44]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testChangeOfPartitionColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType());
        RowType.RowField afterFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldB = new RowType.RowField("b", new BigIntType());

        System.clearProperty("datatype.cast.allow_precision_loss");
        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.singletonList("b"), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterFieldA.getName(), afterFieldA.getType().asSerializableString())
                                .column(afterFieldB.getName(), afterFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111, 11), (2.2222222222, 22)",
                "insert into test_sink values (3.33333333333, 33), (4.4444444444, 44)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, true, null, null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, BIGINT, true, null, null, null]]",
                "[+I[1.1111112, 11], +I[2.2222223, 22]]",
                "[+I[1.1111112, null], +I[2.2222223, null], +I[3.3333333, 33], +I[4.4444447, 44]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testChangeDatatypeOfPrimaryKeyColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType(false));
        RowType.RowField afterFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldB = new RowType.RowField("b", new BigIntType(false));

        System.clearProperty("datatype.cast.allow_precision_loss");
        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .primaryKey("b")
                                .build(),
                        "", Collections.emptyList(), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterFieldA.getName(), afterFieldA.getType().asSerializableString())
                                .column(afterFieldB.getName(), afterFieldB.getType().asSerializableString())
                                .primaryKey("b")
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111, 11), (2.2222222222, 22)",
                "insert into test_sink values (3.33333333333, 33), (4.4444444444, 44)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, false, PRI(b), null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, BIGINT, false, PRI(b), null, null]]",
                "[+I[1.1111112, 11], +I[2.2222223, 22]]",
                "[+I[1.1111112, null], +I[2.2222223, null], +I[3.3333333, 33], +I[4.4444447, 44]]"
        );
    }

    @Test(expected = ExecutionException.class)
    public void testChangeOfPrimaryKeyColumn() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField beforeFieldB = new RowType.RowField("b", new IntType(false));
        RowType.RowField afterFieldA = new RowType.RowField("a", new FloatType());
        RowType.RowField afterFieldB = new RowType.RowField("b", new BigIntType());

        System.clearProperty("datatype.cast.allow_precision_loss");
        System.clearProperty("datatype.cast.allow_precision_inc");
        testSchemaMigration(
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(beforeFieldA.getName(), beforeFieldA.getType().asSerializableString())
                                .column(beforeFieldB.getName(), beforeFieldB.getType().asSerializableString())
                                .primaryKey("b")
                                .build(),
                        "", Collections.emptyList(), options),
                CatalogTable.of(
                        Schema.newBuilder()
                                .column(afterFieldA.getName(), afterFieldA.getType().asSerializableString())
                                .column(afterFieldB.getName(), afterFieldB.getType().asSerializableString())
                                .build(),
                        "", Collections.emptyList(), options),
                "insert into test_sink values (1.1111111111, 11), (2.2222222222, 22)",
                "insert into test_sink values (3.33333333333, 33), (4.4444444444, 44)",
                "[+I[a, FLOAT, true, null, null, null], +I[b, INT, false, PRI(b), null, null]]",
                "[+I[a, FLOAT, true, null, null, null], +I[b, BIGINT, false, PRI(b), null, null]]",
                "[+I[1.1111112, 11], +I[2.2222223, 22]]",
                "[+I[1.1111112, null], +I[2.2222223, null], +I[3.3333333, 33], +I[4.4444447, 44]]"
        );
    }

    void testSchemaMigration(CatalogTable beforeTable,
                             CatalogTable afterTable,
                             String beforeInsertSql,
                             String afterInsertSql,
                             String beforeExpectedDescription,
                             String afterExpectedDescription,
                             String beforeExpectedValue,
                             String afterExpectedValue) throws ExecutionException, InterruptedException {
        testCatalog.cleanForTest();
        testCatalog.setCurrentTable(beforeTable);
        testEnv.executeSql(beforeInsertSql).await();
        List<Row> desc_test_sink_before = CollectionUtil.iteratorToList(validateEnv.executeSql("desc test_sink").collect());
        if (beforeExpectedDescription != null)
            assertThat(desc_test_sink_before.toString()).isEqualTo(beforeExpectedDescription);
        List<Row> select_test_sink_before = CollectionUtil.iteratorToList(validateEnv.executeSql("select * from test_sink").collect());
        select_test_sink_before.sort(Comparator.comparing(Row::toString));
        if (beforeExpectedValue != null)
            assertThat(select_test_sink_before.toString()).isEqualTo(beforeExpectedValue);
        validateEnv.executeSql("select * from test_sink").print();

        testCatalog.setCurrentTable(afterTable);
        testEnv.executeSql(afterInsertSql).await();
        List<Row> desc_test_sink_after = CollectionUtil.iteratorToList(validateEnv.executeSql("desc test_sink").collect());
        if (afterExpectedDescription != null)
            assertThat(desc_test_sink_after.toString()).isEqualTo(afterExpectedDescription);
        List<Row> select_test_sink_after = CollectionUtil.iteratorToList(validateEnv.executeSql("select * from test_sink").collect());
        select_test_sink_before.sort(Comparator.comparing(Row::toString));
        if (afterExpectedValue != null)
            assertThat(select_test_sink_after.toString()).isEqualTo(afterExpectedValue);
        validateEnv.executeSql("select * from test_sink").print();
    }

    static class CreateTableAtSinkCatalog extends LakeSoulCatalog {

        private CatalogBaseTable currentTable;

        public void setCurrentTable(CatalogBaseTable currentTable) {
            this.currentTable = currentTable;
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
            System.out.println(currentTable.getUnresolvedSchema());
            System.out.println(currentTable.getOptions());
            return currentTable;
        }
    }
}
