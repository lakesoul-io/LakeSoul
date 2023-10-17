// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.dao.TableInfoDao;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.util.PrestoUtil;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.arrow.ArrowUtils;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.lakesoul.util.PrestoUtil.CDC_CHANGE_COLUMN;

public class LakeSoulMetadata implements ConnectorMetadata {

    private final DBManager dbManager = new DBManager();

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return dbManager.listNamespaces();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        String namespace = schemaName.orElse("default");
        return dbManager.listTableNamesByNamespace(namespace)
                .stream()
                .map(name -> new SchemaTableName(namespace, name))
                .collect(Collectors.toList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        TableInfo
                tableInfo =
                dbManager.getTableInfoByNameAndNamespace(tableName.getTableName(), tableName.getSchemaName());

        if (tableInfo == null) {
            throw new RuntimeException("no such table: " + tableName);
        }

        LakeSoulTableHandle lakeSoulTableHandle = new LakeSoulTableHandle(
                tableInfo.getTableId(),
                tableName
        );

        return lakeSoulTableHandle;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(
            ConnectorSession session, ConnectorTableHandle table,
            Constraint<ColumnHandle> constraint,
            Optional<Set<ColumnHandle>> desiredColumns) {
        LakeSoulTableHandle tableHandle = (LakeSoulTableHandle) table;
        TableInfo tableInfo = dbManager.getTableInfoByTableId(((LakeSoulTableHandle) table).getId());
        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        JSONObject properties = JSON.parseObject(tableInfo.getProperties());
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
        if (TableInfoDao.isArrowKindSchema(tableInfo.getTableSchema())) {
            try {
                arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            StructType struct = (StructType) StructType.fromJson(tableInfo.getTableSchema());
            arrowSchema = org.apache.spark.sql.arrow.ArrowUtils.toArrowSchema(struct, ZoneId.of("UTC").toString());
        }
        HashMap<String, ColumnHandle> allColumns = new HashMap<>();
        String cdcChangeColumn = properties.getString(CDC_CHANGE_COLUMN);
        for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
            // drop cdc change column
            if (cdcChangeColumn != null && field.getName().equals(cdcChangeColumn)) {
                continue;
            }
            LakeSoulTableColumnHandle columnHandle =
                    new LakeSoulTableColumnHandle(tableHandle,
                            field.getName(),
                            PrestoUtil.convertToPrestoType(field.getType()));
            allColumns.put(field.getName(), columnHandle);
        }
        ConnectorTableLayout layout = new ConnectorTableLayout(
                new LakeSoulTableLayoutHandle(
                        tableHandle,
                        desiredColumns,
                        partitionKeys.primaryKeys,
                        partitionKeys.rangeKeys,
                        properties,
                        constraint.getSummary(),
                        allColumns
                )
        );
        return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        LakeSoulTableHandle handle = (LakeSoulTableHandle) table;
        if (!listSchemaNames(session).contains(handle.getNames().getSchemaName())) {
            return null;
        }

        TableInfo tableInfo = dbManager.getTableInfoByTableId(handle.getId());
        if (tableInfo == null) {
            throw new RuntimeException("no such table: " + handle.getNames());
        }
        JSONObject properties = JSON.parseObject(tableInfo.getProperties());
        org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
        if (TableInfoDao.isArrowKindSchema(tableInfo.getTableSchema())) {
            try {
                arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            StructType struct = (StructType) StructType.fromJson(tableInfo.getTableSchema());
            arrowSchema = org.apache.spark.sql.arrow.ArrowUtils.toArrowSchema(struct, ZoneId.of("UTC").toString());
        }

        List<ColumnMetadata> columns = new LinkedList<>();
        String cdcChangeColumn = properties.getString(CDC_CHANGE_COLUMN);
        for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
            Map<String, Object> props = new HashMap<>();
            for (Map.Entry<String, String> entry : field.getMetadata().entrySet()) {
                props.put(entry.getKey(), entry.getValue());
            }
            // drop cdc change column
            if (cdcChangeColumn != null && field.getName().equals(cdcChangeColumn)) {
                continue;
            }
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    field.getName(),
                    PrestoUtil.convertToPrestoType(field.getType()),
                    field.isNullable(),
                    "",
                    "",
                    false,
                    props
            );
            columns.add(columnMetadata);
        }

        return new ConnectorTableMetadata(
                handle.getNames(),
                columns,
                properties,
                Optional.of("")
        );
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        LakeSoulTableHandle table = (LakeSoulTableHandle) tableHandle;
        TableInfo tableInfo = dbManager.getTableInfoByTableId(table.getId());
        if (tableInfo == null) {
            throw new RuntimeException("no such table: " + table.getNames());
        }
        JSONObject properties = JSON.parseObject(tableInfo.getProperties());

        org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
        if (TableInfoDao.isArrowKindSchema(tableInfo.getTableSchema())) {
            try {
                arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            StructType struct = (StructType) StructType.fromJson(tableInfo.getTableSchema());
            arrowSchema = org.apache.spark.sql.arrow.ArrowUtils.toArrowSchema(struct, ZoneId.of("UTC").toString());
        }

        HashMap<String, ColumnHandle> map = new HashMap<>();
        String cdcChangeColumn = properties.getString(CDC_CHANGE_COLUMN);
        for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
            // drop cdc change column
            if (cdcChangeColumn != null && field.getName().equals(cdcChangeColumn)) {
                continue;
            }
            LakeSoulTableColumnHandle columnHandle =
                    new LakeSoulTableColumnHandle(table,
                            field.getName(),
                            PrestoUtil.convertToPrestoType(field.getType()));
            map.put(field.getName(), columnHandle);
        }
        return map;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) columnHandle;
        TableInfo tableInfo = dbManager.getTableInfoByTableId(handle.getTableHandle().getId());
        if (tableInfo == null) {
            throw new RuntimeException("no such table: " + handle.getTableHandle().getNames());
        }

        org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
        if (TableInfoDao.isArrowKindSchema(tableInfo.getTableSchema())) {
            try {
                arrowSchema = Schema.fromJSON(tableInfo.getTableSchema());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            StructType struct = (StructType) StructType.fromJson(tableInfo.getTableSchema());
            arrowSchema = org.apache.spark.sql.arrow.ArrowUtils.toArrowSchema(struct, ZoneId.of("UTC").toString());
        }
        for (org.apache.arrow.vector.types.pojo.Field field : arrowSchema.getFields()) {
            Map<String, Object> properties = new HashMap<>();
            for (Map.Entry<String, String> entry : field.getMetadata().entrySet()) {
                properties.put(entry.getKey(), entry.getValue());
            }
            if (field.getName().equals(handle.getColumnName())) {
                return new ColumnMetadata(
                        field.getName(),
                        PrestoUtil.convertToPrestoType(field.getType()),
                        field.isNullable(),
                        "",
                        "",
                        false,
                        properties
                );
            }
        }

        throw new RuntimeException("no such column: " + handle.getColumnName());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        //prefix: lakesoul.default.table1
        String schema = prefix.getSchemaName();
        String tableNamePrefix = prefix.getTableName();
        List<String> tableNames = dbManager.listTableNamesByNamespace(schema);
        Map<SchemaTableName, List<ColumnMetadata>> results = new HashMap<>();
        for (String tableName : tableNames) {
            if (tableName.startsWith(tableNamePrefix)) {
                SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
                ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName);
                ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
                results.put(schemaTableName, tableMetadata.getColumns());
            }
        }
        return results;
    }
}
