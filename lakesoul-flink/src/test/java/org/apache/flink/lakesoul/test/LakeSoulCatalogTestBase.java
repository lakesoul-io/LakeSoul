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
package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.flink.table.catalog.Catalog;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

    @Parameterized.Parameters(name = "catalogName = {0} baseNamespace = {1}")
    public static Iterable<Object[]> parameters() {
        return Lists.newArrayList(
                new Object[]{"lakesoul", Namespace.defaultNamespace()},
                new Object[]{"lakesoul", Namespace.defaultNamespace()});
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
        this.flinkTablePath = "file:/tmp/" + flinkTable;
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
