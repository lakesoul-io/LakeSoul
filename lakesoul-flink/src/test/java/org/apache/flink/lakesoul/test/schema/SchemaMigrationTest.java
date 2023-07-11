package org.apache.flink.lakesoul.test.schema;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.LakeSoulTestUtils;
import org.apache.flink.lakesoul.test.MockLakeSoulCatalog;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
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
    public void testSchemeMigration() throws IOException, ExecutionException, InterruptedException {
        Map<String, String> options = new HashMap<>();
        testCatalog.cleanForTest();
        options.put(CATALOG_PATH.key(), tempFolder.newFolder("test_sink").getAbsolutePath());
        RowType.RowField field = new RowType.RowField("a", new IntType());
        testCatalog.setCurrentTable(CatalogTable.of(Schema.newBuilder().column(field.getName(), field.getType().asSerializableString()).build(), "", Collections.emptyList(), options));
        testEnv.executeSql("insert into test_sink values (1), (2)").await();
        List<Row> desc_test_sink_before = CollectionUtil.iteratorToList(validateEnv.executeSql("desc test_sink").collect());
        assertThat(desc_test_sink_before.toString()).isEqualTo("[+I[a, INT, true, null, null, null]]");
        List<Row> select_test_sink_before = CollectionUtil.iteratorToList(validateEnv.executeSql("select * from test_sink").collect());
        select_test_sink_before.sort(Comparator.comparing(Row::toString));
        assertThat(select_test_sink_before.toString()).isEqualTo("[+I[1], +I[2]]");
        validateEnv.executeSql("select * from test_sink").print();

        field = new RowType.RowField("a", new BigIntType());
        testCatalog.setCurrentTable(CatalogTable.of(Schema.newBuilder().column(field.getName(), field.getType().asSerializableString()).build(), "", Collections.emptyList(), options));
        testEnv.executeSql("insert into test_sink values (3), (4)").await();
        List<Row> desc_test_sink_after = CollectionUtil.iteratorToList(validateEnv.executeSql("desc test_sink").collect());
        assertThat(desc_test_sink_after.toString()).isEqualTo("[+I[a, BIGINT, true, null, null, null]]");
        List<Row> select_test_sink_after = CollectionUtil.iteratorToList(validateEnv.executeSql("select * from test_sink").collect());
        select_test_sink_before.sort(Comparator.comparing(Row::toString));
        assertThat(select_test_sink_after.toString()).isEqualTo("[+I[1], +I[2], +I[3], +I[4]]");
        validateEnv.executeSql("select * from test_sink").print();
    }

    static class CreateTableAtSinkCatalog extends LakeSoulCatalog {

        private CatalogBaseTable currentTable;

        public void setCurrentTable(CatalogBaseTable currentTable) {
            this.currentTable = currentTable;
        }

        @Override
        public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
            return currentTable;
        }
    }
}
