/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.lakesoul.source.LakeSoulLookupTableSource;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.*;
import org.apache.flink.table.types.logical.RowType;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CATALOG_PATH;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.FACTORY_IDENTIFIER;

public class LakeSoulDynamicTableFactory implements DynamicTableSinkFactory, DynamicTableSourceFactory {

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        Configuration options = (Configuration) FactoryUtil.createTableFactoryHelper(this, context).getOptions();
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
                discoverEncodingFormat(context, BulkWriterFormatFactory.class),
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

    private <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverEncodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private boolean formatFactoryExists(Context context, Class<?> factoryClass) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        String identifier = options.get(FactoryUtil.FORMAT);
        if (!"lakesoul".equals(identifier)) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FactoryUtil.FORMAT.key()));
        }

        final List<Factory> factories = new LinkedList<>();
        ServiceLoader.load(Factory.class, context.getClassLoader())
                .iterator()
                .forEachRemaining(factories::add);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(identifier))
                        .collect(Collectors.toList());

        return !matchingFactories.isEmpty();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        Configuration options = (Configuration) FactoryUtil.createTableFactoryHelper(this, context).getOptions();
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
                (RowType) catalogTable.getResolvedSchema().toSourceRowDataType().notNull().getLogicalType(), isStreaming, pkColumns, catalogTable, options.toMap()
        );
    }
}
