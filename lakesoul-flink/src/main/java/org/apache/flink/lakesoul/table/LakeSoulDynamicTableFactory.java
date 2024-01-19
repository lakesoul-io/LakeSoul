// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.lakesoul.source.LakeSoulLookupTableSource;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import java.util.*;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CATALOG_PATH;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.FACTORY_IDENTIFIER;

public class LakeSoulDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Configuration options = GlobalConfiguration.loadConfiguration();
        options.addAll((Configuration) FactoryUtil.createTableFactoryHelper(this, context).getOptions());
        FlinkUtil.setLocalTimeZone(options,
                FlinkUtil.getLocalTimeZone(((TableConfig) context.getConfiguration()).getConfiguration()));

        ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        TableSchema schema = catalogTable.getSchema();
        List<String> pkColumns = schema.getPrimaryKey().map(UniqueConstraint::getColumns).orElse(new ArrayList<>());

        return new LakeSoulTableSink(
                objectIdentifier.asSummaryString(),
                objectIdentifier.getObjectName(),
                catalogTable.getResolvedSchema().toPhysicalRowDataType(),
                pkColumns, catalogTable.getPartitionKeys(),
                options,
                context.getCatalogTable().getResolvedSchema()
        );
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(CATALOG_PATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = GlobalConfiguration.loadConfiguration();
        options.addAll((Configuration) FactoryUtil.createTableFactoryHelper(this, context).getOptions());
        FlinkUtil.setLocalTimeZone(options,
                FlinkUtil.getLocalTimeZone(((TableConfig) context.getConfiguration()).getConfiguration()));
        ObjectIdentifier objectIdentifier = context.getObjectIdentifier();
        ResolvedCatalogTable catalogTable = context.getCatalogTable();
        TableSchema schema = catalogTable.getSchema();
        List<String> pkColumns;
        if (schema.getPrimaryKey().isPresent()) {
            pkColumns = schema.getPrimaryKey().get().getColumns();
        } else {
            pkColumns = new ArrayList<>();
        }
        catalogTable.getPartitionKeys();
        boolean isStreaming = true;
        final RuntimeExecutionMode mode = context.getConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        if (mode == RuntimeExecutionMode.AUTOMATIC) {
            throw new RuntimeException(
                    String.format("Runtime execution mode '%s' is not supported yet.", mode));
        } else {
            if (mode == RuntimeExecutionMode.BATCH) {
                isStreaming = false;
            }
        }

        return new LakeSoulLookupTableSource(
                new TableId(io.debezium.relational.TableId.parse(objectIdentifier.asSummaryString())),
                (RowType) catalogTable.getResolvedSchema().toSourceRowDataType().notNull().getLogicalType(),
                isStreaming,
                pkColumns,
                catalogTable,
                options.toMap()
        );
    }
}
