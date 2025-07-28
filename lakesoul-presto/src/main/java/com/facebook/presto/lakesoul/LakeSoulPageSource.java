// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.facebook.presto.lakesoul.pojo.Path;
import com.facebook.presto.lakesoul.util.ArrowBlockBuilder;
import com.facebook.presto.lakesoul.util.PrestoUtil;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class LakeSoulPageSource implements ConnectorPageSource {
    private static final Logger log = Logger.get(LakeSoulSplitManager.class);

    private final LakeSoulSplit split;
    private final ArrowBlockBuilder arrowBlockBuilder;
    private final List<LakeSoulTableColumnHandle> columns;
    private LakeSoulArrowReader reader;
    private int currentPositition = 0;
    private VectorSchemaRoot currentVCR;
    LinkedHashMap<String, String> partitions;
    private boolean isFinished = false;

    public LakeSoulPageSource(LakeSoulSplit split, ArrowBlockBuilder arrowBlockBuilder,
                              List<LakeSoulTableColumnHandle> columns) throws IOException {
        this.split = split;
        this.arrowBlockBuilder = arrowBlockBuilder;
        this.columns = columns;
        NativeIOReader reader = new NativeIOReader();
        // set paths, schema, pks
        for (Path path : split.getPaths()) {
            reader.addFile(path.getFilename());
        }
        this.partitions = PrestoUtil.extractPartitionSpecFromPath(split.getPaths().get(0));

        List<Field> fields = columns.stream().map(columnHandle -> {
            try {
                return columnHandle.getArrowField();
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());
        HashMap<String, ColumnHandle> allcolumns = split.getLayout().getAllColumns();
        List<String> dataCols = columns.stream().map(LakeSoulTableColumnHandle::getColumnName).collect(Collectors.toList());
        // add extra pks
        List<String> prikeys = split.getLayout().getPrimaryKeys();
        for (String item : prikeys) {
            if (!dataCols.contains(item)) {
                LakeSoulTableColumnHandle columnHandle = (LakeSoulTableColumnHandle) allcolumns.get(item);
                try {
                    fields.add(columnHandle.getArrowField());
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        // add extra cdc column
        String cdcColumn =
                split.getLayout().getTableParameters().getString(PrestoUtil.CDC_CHANGE_COLUMN);
        if (cdcColumn != null) {
            fields.add(Field.notNullable(cdcColumn, new ArrowType.Utf8()));
            reader.addFilter(FilterApi.notEq(
                            FilterApi.binaryColumn(cdcColumn),
                            Binary.fromString("delete"))
                    .toString());
        }

        reader.setPrimaryKeys(prikeys);
        reader.setSchema(new Schema(fields));
        for (Map.Entry<String, String> partition : this.partitions.entrySet()) {
            reader.setDefaultColumnValue(partition.getKey(), partition.getValue());
        }
        // set filters
        split.getLayout().getFilters().forEach((filter) -> reader.addFilter(filter.toString()));
        // set s3 options
        reader.setObjectStoreOptions(
                LakeSoulConfig.getInstance().getAccessKey(),
                LakeSoulConfig.getInstance().getAccessSecret(),
                LakeSoulConfig.getInstance().getRegion(),
                LakeSoulConfig.getInstance().getBucketName(),
                LakeSoulConfig.getInstance().getEndpoint(),
                LakeSoulConfig.getInstance().getSigner(),
                LakeSoulConfig.getInstance().getDefaultFS(),
                LakeSoulConfig.getInstance().getUser(),
                LakeSoulConfig.getInstance().isVirtualPathStyle()
        );

        // init reader
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader,
                100000);
        log.info("Initialized LakeSoulPageSource {}", this);
    }

    @Override public long getCompletedBytes() {
        return 0;
    }

    @Override public long getCompletedPositions() {
        return currentPositition;
    }

    @Override public long getReadTimeNanos() {
        return 0;
    }

    @Override public boolean isFinished() {
        return isFinished;
    }

    @Override public Page getNextPage() {
        if (this.currentVCR != null) {
            this.currentVCR.close();
        }

        if (this.reader.hasNext()) {
            ++currentPositition;
            this.currentVCR = this.reader.nextResultVectorSchemaRoot();
            List<Block> blocks = new ArrayList<>();
            List<FieldVector> vectors = currentVCR.getFieldVectors();
            for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
                FieldVector vector = vectors.get(columnIndex);
                Type type = columns.get(columnIndex).getColumnType();
                Block block = arrowBlockBuilder.buildBlockFromFieldVector(vector, type, reader.reader().getProvider());
                blocks.add(block);
            }

            return new Page(currentVCR.getRowCount(), blocks.toArray(new Block[0]));
        } else {
            isFinished = true;
            return null;
        }
    }

    @Override public long getSystemMemoryUsage() {
        return 0;
    }

    @Override public void close() throws IOException {
        if (this.currentVCR != null) {
            this.currentVCR.close();
            this.currentVCR = null;
        }
        if (this.reader != null) {
            this.reader.close();
            this.reader = null;
        }
    }

    @Override public CompletableFuture<?> isBlocked() {
        return ConnectorPageSource.super.isBlocked();
    }

    @Override public RuntimeStats getRuntimeStats() {
        return ConnectorPageSource.super.getRuntimeStats();
    }

    @Override public String toString() {
        return "LakeSoulPageSource{" +
                "split=" + split +
                ", columns=" + columns +
                ", partitions=" + partitions +
                '}';
    }
}
