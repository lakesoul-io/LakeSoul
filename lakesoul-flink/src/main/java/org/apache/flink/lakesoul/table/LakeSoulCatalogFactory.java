// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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
