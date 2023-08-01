/*
 * // SPDX-FileCopyrightText: 2023 LakeSoul Contributors
 * //
 * // SPDX-License-Identifier: Apache-2.0
 */

package com.facebook.presto.lakesoul;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class LakeSoulMetadata implements ConnectorMetadata {
    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return null;
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return null;
    }

    @Override
    public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session, ConnectorTableHandle table, Constraint<ColumnHandle> constraint, Optional<Set<ColumnHandle>> desiredColumns) {
        return null;
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle) {
        return null;
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return null;
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
