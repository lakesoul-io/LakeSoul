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

package org.apache.flink.lakesoul.table;

import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.Map;
import java.util.Set;
import java.util.Collections;

public class LakeSoulCatalogFactory implements TableFactory, CatalogFactory {

  @Override
  public Catalog createCatalog(String name, Map<String, String> properties) {
    return new LakeSoulCatalog();
  }
  @Override
  public Catalog createCatalog(CatalogFactory.Context context) {
    return new LakeSoulCatalog();
  }

  @Override
  public String factoryIdentifier() {
    return "lakesoul";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

}
