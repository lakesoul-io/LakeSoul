/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;
import org.apache.flink.table.factories.CatalogFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collections;
import org.apache.flink.table.planner.delegation.BatchPlanner;
import org.apache.flink.table.planner.delegation.StreamPlanner;
public class LakesoulCatalogFactory  implements TableFactory,CatalogFactory {

    public Catalog createCatalog(String name, Map<String, String> properties) {
         return new LakesoulCatalog();
    }
      @Override
      public Catalog createCatalog(CatalogFactory.Context context) {
         return new LakesoulCatalog();
    }
    @Override
    public Map<String, String> requiredContext() {
//        Map context = new HashMap<String, String>();
//        context.put("type", "lakesoul");
//        return context;
        return null;
    }

    @Override
    public List<String> supportedProperties() {
        return null;
    }

    @Override
    public String factoryIdentifier(){
        return "lakesouls";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
         return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    /*@Override
    public Executor create(Configuration configuration) {
        return null;
    }

    @Override
    public Planner create(PlannerFactory.Context context) {
        final RuntimeExecutionMode runtimeExecutionMode =
                context.getTableConfig().getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        switch (runtimeExecutionMode) {
            case STREAMING:
                return new StreamPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager());
            case BATCH:
                return new BatchPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager());
            default:
                throw new TableException(
                        String.format(
                                "Unknown runtime mode '%s'. This is a bug. Please consider filing an issue.",
                                runtimeExecutionMode));
        }
    }*/

}
