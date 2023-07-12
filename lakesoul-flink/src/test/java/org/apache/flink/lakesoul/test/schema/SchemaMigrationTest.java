package org.apache.flink.lakesoul.test.schema;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
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
    public void testFromFloatToDouble() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new FloatType());
        RowType.RowField afterField = new RowType.RowField("a", new DoubleType());

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
    public void testFromDoubleToFloat() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField beforeField = new RowType.RowField("a", new DoubleType());
        RowType.RowField afterField = new RowType.RowField("a", new FloatType());

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

    void testSchemaMigration(CatalogTable beforeTable,
                             CatalogTable afterTable,
                             String beforeInsertSql,
                             String afterInsertSql,
                             String beforeExpectedDescription,
                             String afterExpectedDescription,
                             String beforeExpectedValue,
                             String afterExpectedValue) throws IOException, ExecutionException, InterruptedException {
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
            return currentTable;
        }
    }
}
