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

import com.dmetasoul.lakesoul.meta.DBManager;
import com.google.common.collect.Lists;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.util.List;

public class LakeSoulFlinkTestBase extends AbstractTestBase {

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static DBManager dbManager = null;
    protected static LakeSoulCatalog catalog = null;

    private volatile TableEnvironment tEnv = null;

    @BeforeClass
    public static void startDBManager() {
        System.out.println("Initializing DBManager");
        LakeSoulFlinkTestBase.dbManager = new DBManager();
        dbManager.cleanMeta();
        LakeSoulFlinkTestBase.catalog = new LakeSoulCatalog();
    }

    @AfterClass
    public static void stopMetastore() throws Exception {
        dbManager.cleanMeta();
        LakeSoulFlinkTestBase.catalog = null;
    }

    protected TableEnvironment getTableEnv() {
        if (tEnv == null) {
            synchronized (this) {
                if (tEnv == null) {
                    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();

                    TableEnvironment env = TableEnvironment.create(settings);
                    env.getConfig()
                       .getConfiguration()
                    ;
                    env.registerCatalog("lakesoul", catalog);

                    tEnv = env;
                }
            }
        }
        return tEnv;
    }

    protected static TableResult exec(TableEnvironment env, String query, Object... args) {
        return env.executeSql(String.format(query, args));
    }

    protected TableResult exec(String query, Object... args) {
        return exec(getTableEnv(), query, args);
    }

    protected List<Row> sql(String query, Object... args) {
        System.out.println("executing flink sql: " + String.format(query, args));
        TableResult tableResult = exec(query, args);
        try (CloseableIterator<Row> iter = tableResult.collect()) {
            return Lists.newArrayList(iter);
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect table result", e);
        }
    }

    protected void assertSameElements(Iterable<Row> expected, Iterable<Row> actual) {
        Assertions.assertThat(actual).isNotNull().containsExactlyInAnyOrderElementsOf(expected);
    }

    protected void assertSameElements(String message, Iterable<Row> expected, Iterable<Row> actual) {
        Assertions.assertThat(actual)
                  .isNotNull()
                  .as(message)
                  .containsExactlyInAnyOrderElementsOf(expected);
    }

}
