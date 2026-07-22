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
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableLayoutHandle;
import com.facebook.presto.lakesoul.util.ArrowBlockBuilder;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static com.facebook.presto.lakesoul.util.PrestoUtil.CDC_CHANGE_COLUMN;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;

public class LakeSoulMetadata implements ConnectorMetadata {

    private final DBManager dbManager = new DBManager();

    private final ArrowBlockBuilder typeConverter;

    public LakeSoulMetadata(TypeManager typeManager) {
        this.typeConverter = new ArrowBlockBuilder(typeManager);
    }

    private static boolean isCaseSensitiveNameMatching() {
        LakeSoulConfig config = LakeSoulConfig.getInstance();
        return config != null && config.isCaseSensitiveNameMatching();
    }

    @Override
    public String normalizeIdentifier(ConnectorSession session, String identifier) {
        return isCaseSensitiveNameMatching() ? identifier : identifier.toLowerCase(Locale.ENGLISH);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return dbManager.listNamespaces().stream()
                .map(namespace -> normalizeIdentifier(session, namespace))
                .distinct()
                .collect(Collectors.toList());
    }

    private String resolvePhysicalNamespace(ConnectorSession session, String logicalNamespace) {
        if (isCaseSensitiveNameMatching()) {
            return dbManager.listNamespaces().contains(logicalNamespace)
                    ? logicalNamespace
                    : null;
        }

        List<String> matchedNamespaces = dbManager.listNamespaces().stream()
                .filter(namespace -> normalizeIdentifier(session, namespace).equals(logicalNamespace))
                .collect(Collectors.toList());

        if (matchedNamespaces.size() > 1) {
            throw new PrestoException(
                    NOT_SUPPORTED,
                    String.format(
                            "Multiple LakeSoul schemas match case-insensitive name '%s': %s. " +
                                    "Set case-sensitive-name-matching=true to distinguish them.",
                            logicalNamespace,
                            matchedNamespaces));
        }

        return matchedNamespaces.isEmpty() ? null : matchedNamespaces.get(0);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        String logicalNamespace = schemaName.orElse("default");
        String physicalNamespace = resolvePhysicalNamespace(session, logicalNamespace);
        if (physicalNamespace == null) {
            return Collections.emptyList();
        }

        return dbManager.listTableNamesByNamespace(physicalNamespace)
                .stream()
                .map(name -> new SchemaTableName(
                        logicalNamespace,
                        normalizeIdentifier(session, name)))
                .collect(Collectors.toList());
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        String physicalNamespace = resolvePhysicalNamespace(session, tableName.getSchemaName());
        if (physicalNamespace == null) {
            return null;
        }

        String physicalTableName = tableName.getTableName();
        if (!isCaseSensitiveNameMatching()) {
            List<String> matchedTableNames = dbManager.listTableNamesByNamespace(physicalNamespace)
                    .stream()
                    .filter(name -> normalizeIdentifier(session, name).equals(tableName.getTableName()))
                    .collect(Collectors.toList());

            if (matchedTableNames.size() > 1) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        String.format(
                                "Multiple LakeSoul tables match case-insensitive name '%s': %s. " +
                                        "Set case-sensitive-name-matching=true to distinguish them.",
                                tableName,
                                matchedTableNames));
            }
            if (!matchedTableNames.isEmpty()) {
                physicalTableName = matchedTableNames.get(0);
            }
        }

        TableInfo
                tableInfo =
                dbManager.getTableInfoByNameAndNamespace(physicalTableName, physicalNamespace);

        if (tableInfo == null) {
            throw new RuntimeException("no such table: " + tableName);
        }

        return new LakeSoulTableHandle(
                tableInfo.getTableId(),
                tableName
        );
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
            if (field.getName().equals(cdcChangeColumn)) {
                continue;
            }
            LakeSoulTableColumnHandle columnHandle =
                    new LakeSoulTableColumnHandle(tableHandle,
                            field.getName(),
                            typeConverter.getPrestoTypeFromArrowField(field),
                            field);
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
            Map<String, Object> props = new HashMap<>(field.getMetadata());
            // drop cdc change column
            if (field.getName().equals(cdcChangeColumn)) {
                continue;
            }

            ColumnMetadata columnMetadata = ColumnMetadata.builder()
                    .setName(normalizeIdentifier(session, field.getName()))
                    .setType(typeConverter.getPrestoTypeFromArrowField(field))
                    .setNullable(field.isNullable())
                    .setComment(field.getMetadata().getOrDefault("spark_comment", ""))
                    .setExtraInfo("")
                    .setHidden(false)
                    .setProperties(Collections.emptyMap())
                    .build();
            columns.add(columnMetadata);
        }

        return new ConnectorTableMetadata(
                handle.getNames(),
                columns,
                Collections.emptyMap(),
                Optional.ofNullable(properties.getString("comment"))
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
            if (field.getName().equals(cdcChangeColumn)) {
                continue;
            }
            String logicalName = normalizeIdentifier(session, field.getName());
            LakeSoulTableColumnHandle columnHandle =
                    new LakeSoulTableColumnHandle(table,
                            field.getName(),
                            typeConverter.getPrestoTypeFromArrowField(field),
                            field);
            map.put(logicalName, columnHandle);
        }
        return map;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) columnHandle;
        Field field = handle.getArrowField();
        Map<String, Object> properties = new HashMap<>(field.getMetadata());
        return ColumnMetadata.builder()
                .setName(normalizeIdentifier(session, handle.getColumnName()))
                .setType(typeConverter.getPrestoTypeFromArrowField(field))
                .setNullable(field.isNullable())
                .setComment(field.getMetadata().getOrDefault("spark_comment", ""))
                .setExtraInfo("")
                .setHidden(false)
                .setProperties(properties)
                .build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
                                                                       SchemaTablePrefix prefix) {
        //prefix: lakesoul.default.table1
        String logicalSchema = prefix.getSchemaName();
        String physicalSchema = resolvePhysicalNamespace(session, logicalSchema);
        if (physicalSchema == null) {
            return Collections.emptyMap();
        }

        String tableNamePrefix = prefix.getTableName();
        List<String> tableNames = dbManager.listTableNamesByNamespace(physicalSchema);
        Map<SchemaTableName, List<ColumnMetadata>> results = new HashMap<>();
        for (String physicalTableName : tableNames) {
            String logicalTableName = normalizeIdentifier(session, physicalTableName);
            if (tableNamePrefix != null &&
                    !logicalTableName.startsWith(normalizeIdentifier(session, tableNamePrefix))) {
                continue;
            }

            SchemaTableName schemaTableName = new SchemaTableName(logicalSchema, logicalTableName);
            ConnectorTableHandle tableHandle = getTableHandle(session, schemaTableName);
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableHandle);
            results.put(schemaTableName, tableMetadata.getColumns());
        }
        return results;
    }

    public TypeManager getTypeManager() {
        return typeConverter.getTypeManager();
    }
}
