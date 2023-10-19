// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.metadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.table.LakeSoulDynamicTableFactory;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import java.io.IOException;
import java.util.*;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_HASH_PARTITION_SPLITTER;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulCatalog implements Catalog {

    public static final String CATALOG_NAME = "lakesoul";
    public static final String TABLE_ID_PREFIX = "table_";
    private static final String TABLE_PATH = "path";
    private final DBManager dbManager;


    public LakeSoulCatalog() {
        dbManager = new DBManager();
        createDatabase("default", new LakesoulCatalogDatabase(), true);
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.of(new LakeSoulDynamicTableFactory());
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return "default";
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return dbManager.listNamespaces();
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {

        Namespace namespaceEntity = dbManager.getNamespaceByNamespace(databaseName);
        if (namespaceEntity == null) {
            throw new DatabaseNotExistException(CATALOG_NAME, databaseName);
        } else {

            Map<String, String> properties = DBUtil.jsonToStringMap(JSON.parseObject(namespaceEntity.getProperties()));

            return new LakesoulCatalogDatabase(properties, namespaceEntity.getComment());
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        Namespace namespaceEntity = dbManager.getNamespaceByNamespace(databaseName);
        return namespaceEntity != null;
    }

    @Override
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfExists)
            throws CatalogException {
        if (databaseExists(databaseName)) {
            if (ignoreIfExists) {
                return;
            }
            throw new CatalogException(String.format("database %s already exists", databaseName));
        }
        try {
            dbManager.createNewNamespace(databaseName, DBUtil.stringMapToJson(catalogDatabase.getProperties()).toJSONString(),
                    catalogDatabase.getComment());
        } catch (RuntimeException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @Override
    public void dropDatabase(String databaseName, boolean ignoreIfNotExists, boolean cascade) throws
            DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(CATALOG_NAME, databaseName);
            } else {
                return;
            }
        }
        List<String> tables = listTables(databaseName);
        if (!tables.isEmpty()) {
            if (cascade) {
                for (String table : tables) {
                    try {
                        dropTable(new ObjectPath(databaseName, table), true);
                    } catch (TableNotExistException e) {
                        throw new CatalogException(e.getMessage(), e.getCause());
                    }
                }
            } else {
                throw new DatabaseNotEmptyException(CATALOG_NAME, databaseName);
            }
        }
        dbManager.deleteNamespace(databaseName);
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(databaseName)) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(CATALOG_NAME, databaseName);
            } else {
                return;
            }
        }
        dbManager.updateNamespaceProperties(databaseName, DBUtil.stringMapToJson(catalogDatabase.getProperties()).toJSONString());
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException {
        List<TableInfo> tifs = dbManager.getTableInfosByNamespace(databaseName);
        List<String> tableNames = new ArrayList<>(100);
        for (TableInfo item : tifs) {
            if (FlinkUtil.isTable(item)) {
                tableNames.add(item.getTableName());
            }
        }
        return tableNames;
    }

    @Override
    public List<String> listViews(String databaseName) throws CatalogException {
        List<TableInfo> tifs = dbManager.getTableInfosByNamespace(databaseName);
        List<String> tableNames = new ArrayList<>(100);
        for (TableInfo item : tifs) {
            if (FlinkUtil.isView(item)) {
                tableNames.add(item.getTableName());
            }
        }
        return tableNames;
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(CATALOG_NAME, tablePath);
        }
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tablePath.getObjectName(), tablePath.getDatabaseName());
        return FlinkUtil.toFlinkCatalog(tableInfo);
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tablePath.getObjectName(), tablePath.getDatabaseName());

        return null != tableInfo;
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        String tableName = tablePath.getObjectName();
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tablePath.getObjectName(), tablePath.getDatabaseName());
        if (tableInfo != null) {
            String tableId = tableInfo.getTableId();
            dbManager.deleteTableInfo(tableInfo.getTablePath(), tableId, tablePath.getDatabaseName());
            dbManager.deleteShortTableName(tableInfo.getTableName(), tableName, tablePath.getDatabaseName());
            dbManager.deleteDataCommitInfo(tableId);
            dbManager.deletePartitionInfoByTableId(tableId);
            if (FlinkUtil.isTable(tableInfo)) {
                Path path = new Path(tableInfo.getTablePath());
                try {
                    path.getFileSystem().delete(path, true);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        } else {
            if (ignoreIfNotExists) {
                return;
            }
            throw new TableNotExistException(CATALOG_NAME, tablePath);
        }
    }

    @Override
    public void renameTable(ObjectPath tablePath, String s, boolean b) throws CatalogException {
        throw new CatalogException("Rename LakeSoul table is not supported for now");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

        checkNotNull(tablePath);
        checkNotNull(table);
        TableSchema schema = table.getSchema();
        Optional<UniqueConstraint> primaryKeyColumns = schema.getPrimaryKey();
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(CATALOG_NAME, tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(CATALOG_NAME, tablePath);
            } else return;
        }
        String primaryKeys = primaryKeyColumns.map(
                        uniqueConstraint -> String.join(LAKESOUL_HASH_PARTITION_SPLITTER,
                                uniqueConstraint.getColumns()))
                .orElse("");
        Map<String, String> tableOptions = table.getOptions();

        // adding cdc options
        if (!"".equals(primaryKeys)) {
            tableOptions.put(HASH_PARTITIONS, primaryKeys);
        }
        Optional<String> cdcColumn;
        if ("true".equals(tableOptions.get(USE_CDC.key()))) {
            if (primaryKeys.isEmpty()) {
                throw new CatalogException("CDC table must have primary key(s)");
            }
            cdcColumn = Optional.of(tableOptions.getOrDefault(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT));
            tableOptions.put(CDC_CHANGE_COLUMN, cdcColumn.get());
        } else {
            cdcColumn = Optional.empty();
        }
        // adding hash bucket options
        if (!primaryKeys.isEmpty()) {
            if (Integer.parseInt(tableOptions.getOrDefault(HASH_BUCKET_NUM.key(), "-1")) <= 0) {
                throw new CatalogException(
                        "Valid integer value for hashBucketNum property must be set for table with primary key");
            }
        }
        String tableId = TABLE_ID_PREFIX + UUID.randomUUID();
        String qualifiedPath = "";
        String sparkSchema = FlinkUtil.toArrowSchema(schema, cdcColumn).toJson();
        List<String> partitionKeys = Collections.emptyList();
        if (table instanceof ResolvedCatalogTable) {
            partitionKeys = ((ResolvedCatalogTable) table).getPartitionKeys();
            String path = null;
            if (tableOptions.containsKey(TABLE_PATH)) {
                path = tableOptions.get(TABLE_PATH);
            } else {
                String flinkWarehouseDir = GlobalConfiguration.loadConfiguration().get(FLINK_WAREHOUSE_DIR);
                if (null != flinkWarehouseDir) {
                    path = String.join("/", flinkWarehouseDir, tablePath.getDatabaseName(), tablePath.getObjectName());
                }
            }
            try {
                FileSystem fileSystem = new Path(path).getFileSystem();
                Path qp = new Path(path).makeQualified(fileSystem);
                FlinkUtil.createAndSetTableDirPermission(qp);
                qualifiedPath = qp.toString();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (table instanceof ResolvedCatalogView) {
            tableOptions.put(LAKESOUL_VIEW.key(), "true");
            tableOptions.put(LAKESOUL_VIEW_TYPE.key(), LAKESOUL_VIEW_TYPE.defaultValue());
            tableOptions.put(VIEW_ORIGINAL_QUERY, ((ResolvedCatalogView) table).getOriginalQuery());
            tableOptions.put(VIEW_EXPANDED_QUERY, ((ResolvedCatalogView) table).getExpandedQuery());
        }
        String json = JSON.toJSONString(tableOptions);
        JSONObject properties = JSON.parseObject(json);
        String tableName = tablePath.getObjectName();
        dbManager.createNewTable(tableId, tablePath.getDatabaseName(), tableName, qualifiedPath, sparkSchema,
                properties, DBUtil.formatTableInfoPartitionsField(primaryKeys, partitionKeys));
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable catalogBaseTable, boolean b) throws CatalogException {
        throw new CatalogException("Alter lakesoul table not supported now");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath)) {
            throw new CatalogException("table path not exist");
        }

        return listPartitions(tablePath, null);
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        if (!tableExists(tablePath)) {
            throw new CatalogException("table path not exist");
        }
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tablePath.getObjectName(), tablePath.getDatabaseName());
        List<PartitionInfo> allPartitionInfo = dbManager.getAllPartitionInfo(tableInfo.getTableId());
        HashSet<String> partitions = new HashSet<>(100);
        for (PartitionInfo pif : allPartitionInfo) {
            partitions.add(pif.getPartitionDesc());
        }
        ArrayList<CatalogPartitionSpec> al = new ArrayList<>(100);
        for (String item : partitions) {
            if (null == item || "".equals(item)) {
                throw new CatalogException("partition not exist");
            } else {
                al.add(new CatalogPartitionSpec(DBUtil.parsePartitionDesc(item)));
            }
        }
        return al;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> list)
            throws CatalogException {
        // TODO: optimize this when filter is an exact match of one partition
        List<CatalogPartitionSpec> partitions = listPartitions(tablePath);
        List<CatalogPartitionSpec> catalogPartitionSpecs = new ArrayList<>();
        for (Expression exp : list) {
            if (exp instanceof CallExpression) {
                if (!"equals".equalsIgnoreCase(
                        ((CallExpression) exp).getFunctionIdentifier().get().getSimpleName().get())) {
                    throw new CatalogException("just support equal;such as range=val and range=val2");
                }
            }
        }
        for (CatalogPartitionSpec cps : partitions) {
            boolean allAnd = true;
            for (Expression exp : list) {
                String key = exp.getChildren().get(0).toString();
                String value = convertFieldType(exp.getChildren().get(1).toString());
                if (cps.getPartitionSpec().containsKey(key) && cps.getPartitionSpec().get(key).equals(value)) {
                    continue;
                } else {
                    allAnd = false;
                    break;
                }
            }
            if (allAnd) {
                catalogPartitionSpecs.add(cps);
            }
        }
        return catalogPartitionSpecs;

    }

    private String convertFieldType(String field) {
        if (field.startsWith("'")) {
            return field.substring(1, field.length() - 1);
        } else {
            return field;
        }
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported");
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        TableInfo tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tablePath.getObjectName(), tablePath.getDatabaseName());
        if (tableInfo == null) {
            throw new CatalogException(tablePath + " does not exist");
        }
        if (tableInfo.getPartitions().equals(LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH)) {
            throw new CatalogException(tablePath + " is not partitioned");
        }
        List<PartitionInfo> partitionInfos = dbManager.getOnePartition(tableInfo.getTableId(),
                DBUtil.formatPartitionDesc(catalogPartitionSpec.getPartitionSpec()));
        return !partitionInfos.isEmpty();
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec,
                                CatalogPartition catalogPartition, boolean ignoreIfExists) throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, boolean ignoreIfExists)
            throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec,
                               CatalogPartition catalogPartition, boolean ignoreIfExists) throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public List<String> listFunctions(String s) throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogFunction getFunction(ObjectPath tablePath) throws CatalogException, FunctionNotExistException {
        throw new FunctionNotExistException("lakesoul", tablePath);
    }

    @Override
    public boolean functionExists(ObjectPath tablePath) throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void createFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b)
            throws CatalogException {

    }

    @Override
    public void alterFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b)
            throws CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void dropFunction(ObjectPath tablePath, boolean b) throws CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) {
        return null;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws CatalogException {
        return null;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath,
                                                         CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return null;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath,
                                                                CatalogPartitionSpec catalogPartitionSpec)
            throws CatalogException {
        return null;
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics catalogTableStatistics, boolean b)
            throws CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics catalogColumnStatistics,
                                           boolean b) throws CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec,
                                         CatalogTableStatistics catalogTableStatistics, boolean b)
            throws CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec,
                                               CatalogColumnStatistics catalogColumnStatistics, boolean b)
            throws CatalogException {
        throw new CatalogException("not supported now");
    }

    public String getName() {
        return CATALOG_NAME;
    }

    public void cleanForTest() {
        dbManager.cleanMeta();
    }
}
