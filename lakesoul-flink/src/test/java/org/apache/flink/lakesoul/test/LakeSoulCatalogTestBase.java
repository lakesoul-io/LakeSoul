// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.dao.NamespaceDao;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.flink.table.catalog.Catalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class LakeSoulCatalogTestBase extends LakeSoulFlinkTestBase {
    protected static final String DATABASE = "test_lakesoul_meta";

    @Before
    public void before() {
        getTableEnv().useCatalog(catalogName);
    }

    @After
    public void clean() {
        getTableEnv().useCatalog("default_catalog");
        sql("DROP CATALOG IF EXISTS %s", catalogName);
    }

    @Test
    public void emptyTest() {
    }

    @Parameterized.Parameters(name = "catalogName = {0} baseNamespace = {1}")
    public static Iterable<Object[]> parameters() {
        return Collections.singletonList(
                new Object[]{"lakesoul", NamespaceDao.DEFAULT_NAMESPACE});
    }

    protected final String catalogName;
    protected final Namespace baseNamespace;
    protected final Catalog validationCatalog;
    protected final String flinkDatabase;

    protected final String flinkTable;

    protected final String flinkTablePath;
    protected final Namespace lakesoulNamespace;

    public LakeSoulCatalogTestBase(String catalogName, Namespace baseNamespace) {
        this.catalogName = catalogName;
        this.baseNamespace = baseNamespace;
        this.validationCatalog = catalog;

        this.flinkDatabase = catalogName + "." + DATABASE;
        this.flinkTable = "test_table";
        this.flinkTablePath = getTempDirUri(flinkTable);
        this.lakesoulNamespace = baseNamespace;
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
