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

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import org.apache.flink.lakesoul.tools.*;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.*;
import org.apache.flink.util.StringUtils;
import org.apache.spark.sql.lakesoul.Snapshot;
import org.apache.spark.sql.lakesoul.SnapshotManagement;
import org.apache.spark.sql.lakesoul.commands.DropTableCommand;
import org.apache.spark.sql.lakesoul.utils.PartitionInfo;
import org.apache.spark.sql.lakesoul.utils.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import scala.Tuple2;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakesoulCatalog implements Catalog {
    private final String LakesoulDatabaseName = "MetaCommon.DATA_BASE()";
    private static final Logger LOG = LoggerFactory.getLogger(LakesoulCatalog.class);
    private String defaultDatabase = "MetaCommon.DATA_BASE()";

    public LakesoulCatalog() {
    }

   /* public LakesoulCatalog(String database) {
        this.defaultDatabase = database;
    }*/

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return Arrays.asList(defaultDatabase);
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
        if (!getDefaultDatabase().equals(databaseName)) {
            throw new DatabaseNotExistException(LakesoulDatabaseName, databaseName);
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
    public void createDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean b) throws DatabaseAlreadyExistException, CatalogException {
        //todo
//        MetaTableManagement.initDatabaseAndTables();
    }

    @Override
    public void dropDatabase(String databaseName, boolean b, boolean b1) throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void alterDatabase(String databaseName, CatalogDatabase catalogDatabase, boolean b) throws DatabaseNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");
        return MetaVersion.listTables();
    }

    @Override
    public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(LakesoulDatabaseName, tablePath);
        }
        String tableName = tablePath.getFullName();
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        return FlinkUtil.toFlinkCatalog( tableInfo );
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        String tableName = tablePath.getFullName();
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        if (MetaVersion.isTableExists(tableInfo.table_name().get())) {
            return true;
        }else{
            return false;
        }
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean b) throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        String tableName = tablePath.getFullName();
        DropTableCommand.dropTable(SnapshotManagement.apply(tableName).snapshot());
    }

    @Override
    public void renameTable(ObjectPath tablePath, String s, boolean b) throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists) throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath);
        checkNotNull(table);
        TableSchema tsc=table.getSchema();
        ResolvedCatalogTable resolTable = (ResolvedCatalogTable) table;
        if (!databaseExists(tablePath.getDatabaseName())) {
            throw new DatabaseNotExistException(LakesoulDatabaseName, tablePath.getDatabaseName());
        }
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(LakesoulDatabaseName, tablePath);
            }
        } else {
            Map<String, String> tableOptions = table.getOptions();
            //todo
//            FlinkUtil.UpdateCassendraInfo(tableOptions);
            String tableName = tablePath.getFullName();
            TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
            //todo
//            MetaVersion.createNewTable(tableName,
//                    tableInfo.table_id(),
//                    FlinkUtil.toSparkSchema( tsc ,FlinkUtil.isLakesoulCdcTable( tableOptions )).json(),
//                    FlinkUtil.getTableRangeColumns(table),
//                    FlinkUtil.getTablePrimaryKey(table),
//                    FlinkUtil.serialOptionsToCasStr(FlinkUtil.UpdateLakesoulCdcTable( tableOptions )),
//                    tableInfo.bucket_num(),
//            );
        }
    }

    @Override
    public void alterTable(ObjectPath tablePath, CatalogBaseTable catalogBaseTable, boolean b) throws TableNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            throw new CatalogException("table path not exist");
        }
        String tableName = tablePath.getFullName();
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        PartitionInfo[] allPartitionInfo = MetaVersion.getAllPartitionInfo(tableInfo.table_id());
        ArrayList cpsList = new ArrayList<CatalogPartitionSpec>();
        for (PartitionInfo pif : allPartitionInfo) {
            //todo
//            cpsList.add(new LakesoulCatalogPartition(FlinkUtil.getRangeValue(pif.range_value()), ""));
        }
        return cpsList;
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> list) throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        if(!partitionExists(tablePath,catalogPartitionSpec)){
            throw new PartitionNotExistException(LakesoulDatabaseName,tablePath,catalogPartitionSpec);
        }
        String rangeValue=FlinkUtil.getRangeValue(catalogPartitionSpec);
        String tableName = tablePath.getFullName();
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        Tuple2<Object, String> partitionId = MetaVersion.getPartitionId(tableInfo.table_id(), rangeValue);
        PartitionInfo pif=MetaVersion.getSinglePartitionInfo(tableInfo.table_id(),rangeValue,partitionId._2);
        //todo
        LakesoulCatalogPartition lcp = null;//new LakesoulCatalogPartition(FlinkUtil.getRangeValue(pif.range_value()),"");
        return lcp;
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws CatalogException {
        checkNotNull(tablePath);
        if (tableExists(tablePath)) {
            throw new CatalogException("table path not exist");
        }
        String rangeValue=FlinkUtil.getRangeValue(catalogPartitionSpec);
        String tableName = tablePath.getFullName();
        //todo
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        //todo rangeValue
        Tuple2<Object, String> partitionId = MetaVersion.getPartitionId(tableInfo.table_id(), rangeValue);
        boolean partitionExisted=(boolean)partitionId._1();
        if(partitionExisted){
            return true;
        }else{
            return false;

        }
    }

    @Override
    public void createPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfExists) throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException, PartitionAlreadyExistsException, CatalogException {
        if(partitionExists(tablePath,catalogPartitionSpec)){
            throw new PartitionAlreadyExistsException(LakesoulDatabaseName,tablePath,catalogPartitionSpec);
        }
        String tableName = tablePath.getFullName();
        Snapshot sh = SnapshotManagement.apply(tableName).snapshot();
        TableInfo tableInfo =sh.getTableInfo();
        PartitionInfo pif = sh.getPartitionInfoArray()[0];
        //todo
//        NewMetaUtil.addPartition(tableInfo.table_id(),tableInfo.table_name(),pif.range_id(),FlinkUtil.getRangeValue(catalogPartitionSpec));
    }

    @Override
    public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, boolean ignoreIfExists) throws PartitionNotExistException, CatalogException {
        if(!partitionExists(tablePath,catalogPartitionSpec)){
            throw new PartitionNotExistException(LakesoulDatabaseName,tablePath,catalogPartitionSpec);
        }
        String tableName = tablePath.getFullName();
        String rangeValue=FlinkUtil.getRangeValue(catalogPartitionSpec);
        TableInfo tableInfo = SnapshotManagement.apply(tableName).getTableInfoOnly();
        Tuple2<Object, String> partitionId = MetaVersion.getPartitionId(tableInfo.table_id(), rangeValue);
        MetaVersion.deletePartitionInfoByRangeId(tableInfo.table_id(),rangeValue,partitionId._2);
    }

    @Override
    public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogPartition catalogPartition, boolean ignoreIfExists) throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogFunction getFunction(ObjectPath tablePath) throws FunctionNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public boolean functionExists(ObjectPath tablePath) throws CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void createFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b) throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterFunction(ObjectPath tablePath, CatalogFunction catalogFunction, boolean b) throws FunctionNotExistException, CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void dropFunction(ObjectPath tablePath, boolean b) throws FunctionNotExistException, CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath) throws TableNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec) throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }

    @Override
    public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics catalogTableStatistics, boolean b) throws TableNotExistException, CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws TableNotExistException, CatalogException, TablePartitionedException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogTableStatistics catalogTableStatistics, boolean b) throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported now");

    }

    @Override
    public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec catalogPartitionSpec, CatalogColumnStatistics catalogColumnStatistics, boolean b) throws PartitionNotExistException, CatalogException {
        throw new CatalogException("not supported now");
    }
}
