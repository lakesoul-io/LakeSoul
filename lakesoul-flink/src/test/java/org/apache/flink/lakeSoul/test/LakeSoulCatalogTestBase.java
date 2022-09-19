package org.apache.flink.lakeSoul.test;

import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.util.ArrayUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class LakeSoulCatalogTestBase extends LakeSoulFlinkTestBase {
    protected static final String DATABASE = "test_lakesoul_meta";

    @Before
    public void before() {
//        sql("CREATE CATALOG %s", catalogName);
//        sql("CREATE CATALOG %s WITH %s", catalogName, toWithClause(config));
        getTableEnv().useCatalog(catalogName);
    }

    @After
    public void clean() {
        getTableEnv().useCatalog("default_catalog");
        sql("DROP CATALOG IF EXISTS %s", catalogName);
    }

    @Parameterized.Parameters(name = "catalogName = {0} baseNamespace = {1}")
    public static Iterable<Object[]> parameters() {
//        return null;
        return Lists.newArrayList(
                new Object[] {"lakesoul", Namespace.defaultNamespace()},
                new Object[] {"lakesoul", Namespace.defaultNamespace()});
    }

    protected final String catalogName;
    protected final Namespace baseNamespace;
    protected final Catalog validationCatalog;
//    protected final SupportsNamespaces validationNamespaceCatalog;
    protected final Map<String, String> config = Maps.newHashMap();

    protected final String flinkDatabase;

    protected final String flinkTable;

    protected final String flinkTablePath;
    protected final Namespace lakesoulNamespace;

    public LakeSoulCatalogTestBase(String catalogName, Namespace baseNamespace) {
        this.catalogName = catalogName;
        this.baseNamespace = baseNamespace;
        this.validationCatalog = catalog;
//        this.validationNamespaceCatalog = (SupportsNamespaces) validationCatalog;

        config.put("type", "lakesoul");
//        if (!baseNamespace.isEmpty()) {
//            config.put(FlinkCatalogFactory.BASE_NAMESPACE, baseNamespace.toString());
//        }
//        if (isHadoopCatalog) {
//            config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hadoop");
//        } else {
//            config.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
//            config.put(CatalogProperties.URI, getURI(hiveConf));
//        }
//        config.put(CatalogProperties.WAREHOUSE_LOCATION, String.format("file://%s", warehouseRoot()));

        this.flinkDatabase = catalogName + "." + DATABASE;
        this.flinkTable = "test_table";
        this.flinkTablePath = "file:/tmp/"+flinkTable;
        this.lakesoulNamespace = baseNamespace;
    }


    protected String getFullQualifiedTableName(String tableName) {
        final List<String> levels = Lists.newArrayList(lakesoulNamespace.getLevels());
        levels.add(tableName);
        return Joiner.on('.').join(levels);
    }



    static String toWithClause(Map<String, String> props) {
        StringBuilder builder = new StringBuilder();
        builder.append("(");
        int propCount = 0;
        for (Map.Entry<String, String> entry : props.entrySet()) {
            if (propCount > 0) {
                builder.append(",");
            }
            builder
                    .append("'")
                    .append(entry.getKey())
                    .append("'")
                    .append("=")
                    .append("'")
                    .append(entry.getValue())
                    .append("'");
            propCount++;
        }
        builder.append(")");
        return builder.toString();
    }
}
