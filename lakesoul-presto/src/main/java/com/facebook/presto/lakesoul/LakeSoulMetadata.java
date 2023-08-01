// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.facebook.presto.lakesoul.handle.LakeSoulTableHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;

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

        List<ColumnMetadata> columns = new LinkedList<>();

        return new ConnectorTableMetadata(handle.getNames(), columns);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }
}

