// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.pojo.TableSchema;
import com.facebook.presto.lakesoul.util.JsonUtil;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import com.facebook.presto.common.type.Type;

import java.util.*;
import java.util.stream.Collectors;

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
        TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(tableName.getTableName(), tableName.getSchemaName());

        if(tableInfo == null) {
            throw new RuntimeException("no such table: " + tableName);
        }

        LakeSoulTableHandle lakeSoulTableHandle = new LakeSoulTableHandle(
                tableInfo.getTableId(),
                tableName
        );

        return lakeSoulTableHandle;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        //LakeSoulTableHandle tableHandle = (LakeSoulTableHandle) table;
        ConnectorTableLayout layout = new ConnectorTableLayout(new LakeSoulTableLayoutHandle());
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
        if(tableInfo == null){
            throw new RuntimeException("no such table: " + handle.getNames());
        }

        TableSchema schema = JsonUtil.parse(tableInfo.getTableSchema(), TableSchema.class);
        List<ColumnMetadata> columns = new LinkedList<>();
        for(TableSchema.Field field : schema.getFields()) {
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    field.getName(),
                    converType(field.getType()),
                    field.isNullable(),
                    "",
                    "",
                    false,
                    field.getMetadata()
            );
            columns.add(columnMetadata);
        }
        Map<String, Object> properties = JsonUtil.parse(tableInfo.getProperties(), Map.class);

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
        if(tableInfo == null){
            throw new RuntimeException("no such table: " + table.getNames());
        }
        TableSchema schema = JsonUtil.parse(tableInfo.getTableSchema(), TableSchema.class);
        HashMap<String, ColumnHandle> map = new HashMap<>();
        for(TableSchema.Field field : schema.getFields()){
            ColumnMetadata columnMetadata = new ColumnMetadata(
                    field.getName(),
                    converType(field.getType()),
                    field.isNullable(),
                    "",
                    "",
                    false,
                    field.getMetadata()
            );

            LakeSoulTableColumnHandle columnHandle =
                    new LakeSoulTableColumnHandle(table, field.getName(), columnMetadata);
            map.put(field.getName(), columnHandle);
        }
        return map;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) columnHandle;
        return handle.getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        //prefix: lakesoul.default.table1
        String schema = prefix.getSchemaName();
        String tableNamePrefix = prefix.getTableName();
        List<String> tableNames = dbManager.listTableNamesByNamespace(schema);
        Map<SchemaTableName, List<ColumnMetadata>> results = new HashMap<>();
        for(String tableName : tableNames){
            if(tableName.startsWith(tableNamePrefix)){
                SchemaTableName schemaTableName = new SchemaTableName(schema, tableName);
                ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName);
                ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
                results.put(schemaTableName, tableMetadata.getColumns());
            }
        }
        return results;
    }


    private Type converType(String type){
        if(type.equals("integer")){
            return IntegerType.INTEGER;
        }else if (type.equals("string")){
            return VarcharType.VARCHAR;
        }else{
            return VarcharType.VARCHAR;
        }
    }
}



