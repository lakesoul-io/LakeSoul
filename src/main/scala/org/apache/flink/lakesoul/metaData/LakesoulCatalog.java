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

package org.apache.flink.lakesoul.metaData;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tools.FlinkUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

import scala.Tuple2;

import static com.dmetasoul.lakesoul.meta.Test.testTablePathId;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.RECORD_KEY_NAME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakesoulCatalog implements Catalog {
  private static final String LAKE_SOUL_DATA_BASE_NAME = "test_lakesoul_meta";
  private static final String TABLE_PATH = "path";
  private static final String TABLE_ID_PREFIX = "table_";
  private DBManager dbManager;

  public LakesoulCatalog() {
  }

  @Override
  public void open() throws CatalogException {
    dbManager = new DBManager();
  }

  @Override
  public void close() throws CatalogException {

  }

  @Override
  public String getDefaultDatabase() throws CatalogException {
    return LAKE_SOUL_DATA_BASE_NAME;
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    return Collections.singletonList(LAKE_SOUL_DATA_BASE_NAME);
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
    if (!getDefaultDatabase().equals(databaseName)) {
      throw new DatabaseNotExistException(LAKE_SOUL_DATA_BASE_NAME, databaseName);
    } else {
      return new LakesoulCatalogDatabase();
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    try {
      getDatabase(databaseName);
      return true;
    } catch (DatabaseNotExistException ignore) {
      return false;
    }
  }

  @Override
  public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean b) throws CatalogException {
  }

  @Override
  public void dropDatabase(String databaseName, boolean b, boolean b1) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public List<String> listTables(String databaseName) throws CatalogException {
    checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName),
        "databaseName cannot be null or empty");
    return dbManager.listTables();
  }

  @Override
  public List<String> listViews(String s) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    if (!tableExists(tablePath)) {
      throw new TableNotExistException(LAKE_SOUL_DATA_BASE_NAME, tablePath);
    }
    String tableName = tablePath.getObjectName();
    TableInfo tableInfo = dbManager.getTableInfoByName(tableName);
    return FlinkUtil.toFlinkCatalog(tableInfo);
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    checkNotNull(tablePath);
    String tableName = tablePath.getObjectName();
    TableInfo tableInfo = dbManager.getTableInfoByName(tableName);


    return null != tableInfo;
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean b) throws TableNotExistException, CatalogException {
    checkNotNull(tablePath);
  }

  @Override
  public void renameTable(ObjectPath tablePath, String s, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    checkNotNull(tablePath);
    checkNotNull(table);
    TableSchema tsc = table.getSchema();
    List<String> columns = tsc.getPrimaryKey().get().getColumns();
    String primaryKeys = FlinkUtil.stringListToString(columns);
    if (!databaseExists(tablePath.getDatabaseName())) {
      throw new DatabaseNotExistException(LAKE_SOUL_DATA_BASE_NAME, tablePath.getDatabaseName());
    }
    if (tableExists(tablePath)) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(LAKE_SOUL_DATA_BASE_NAME, tablePath);
      }
    } else {
      Map<String, String> tableOptions = table.getOptions();
      tableOptions.put(RECORD_KEY_NAME, primaryKeys);
      String json = JSON.toJSONString(tableOptions);
      JSONObject properties = JSON.parseObject(json);
      List<String> partitionKeys = ((ResolvedCatalogTable) table).getPartitionKeys();
      String tableName = tablePath.getObjectName();
      String path = tableOptions.get(TABLE_PATH);
      String qualifiedPath = "";
      try {
        FileSystem fileSystem = new Path(path).getFileSystem();
        qualifiedPath = new Path(path).makeQualified(fileSystem).toString();
      } catch (IOException e) {
        e.printStackTrace();
      }
      String tableId = TABLE_ID_PREFIX + UUID.randomUUID();
      dbManager.createNewTable(tableId, tableName, qualifiedPath,
          FlinkUtil.toSparkSchema(tsc, FlinkUtil.isLakesoulCdcTable(tableOptions)).json(),
          properties, FlinkUtil.stringListToString(partitionKeys)+";"+primaryKeys);
    }
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
    checkNotNull(tablePath);
    if (tableExists(tablePath)) {
      throw new CatalogException("table path not exist");
    }

    return new ArrayList<>();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
      throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> list) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
    if (!partitionExists(tablePath, catalogPartitionSpec)) {
      throw new PartitionNotExistException(LAKE_SOUL_DATA_BASE_NAME, tablePath, catalogPartitionSpec);
    }
    return null;
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
    return false;
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfExists)
      throws PartitionAlreadyExistsException, CatalogException {
    if (partitionExists(tablePath, catalogPartitionSpec)) {
      throw new PartitionAlreadyExistsException(LAKE_SOUL_DATA_BASE_NAME, tablePath, catalogPartitionSpec);
    }
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, boolean ignoreIfExists) throws PartitionNotExistException, CatalogException {
    if (!partitionExists(tablePath, catalogPartitionSpec)) {
      throw new PartitionNotExistException(LAKE_SOUL_DATA_BASE_NAME, tablePath, catalogPartitionSpec);
    }
    String tableName = tablePath.getFullName();
    String rangeValue = FlinkUtil.getRangeValue(catalogPartitionSpec);
    TableInfo tableInfo = dbManager.getTableInfo(tableName);
     }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfExists)
      throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public List<String> listFunctions(String s) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogFunction getFunction(ObjectPath tablePath) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public boolean functionExists(ObjectPath tablePath) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public void createFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public void alterFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public void dropFunction(ObjectPath tablePath, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
    throw new CatalogException("not supported now");
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics catalogTableStatistics, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b)
      throws CatalogException {
    throw new CatalogException("not supported now");

  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b)
      throws CatalogException {
    throw new CatalogException("not supported now");
  }
}
