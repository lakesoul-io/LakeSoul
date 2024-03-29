// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;

public class DynamicPartitionNativeParquetWriter implements InProgressFileWriter<RowData, String> {

    private final RowType rowType;

    private ArrowWriter<RowData> arrowWriter;
    private final List<String> primaryKeys;
    private final List<String> rangeColumns;
    private final Configuration conf;

    private NativeIOWriter nativeWriter;

    private final int batchSize;

    private final long creationTime;

    private VectorSchemaRoot batch;

    private int rowsInBatch;

    long lastUpdateTime;

    String prefix;

    private long totalRows = 0;

    public DynamicPartitionNativeParquetWriter(RowType rowType,
                                               List<String> primaryKeys,
                                               List<String> rangeColumns,
                                               Path path,
                                               long creationTime,
                                               Configuration conf) throws IOException {
        this.batchSize = 250000; // keep same with native writer's row group row number
        this.creationTime = creationTime;
        this.rowsInBatch = 0;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.rangeColumns = rangeColumns;
        this.prefix = path.makeQualified(path.getFileSystem()).toString();
        this.conf = conf;
        initNativeWriter();
    }

    private void initNativeWriter() throws IOException {
        ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema);
        nativeWriter.setPrimaryKeys(primaryKeys);
        nativeWriter.setRangePartitions(rangeColumns);
        if (conf.getBoolean(LakeSoulSinkOptions.isMultiTableSource)) {
            nativeWriter.setAuxSortColumns(Collections.singletonList(SORT_FIELD));
        }
        nativeWriter.setHashBucketNum(conf.getInteger(LakeSoulSinkOptions.HASH_BUCKET_NUM));

        nativeWriter.setRowGroupRowNumber(this.batchSize);
        batch = VectorSchemaRoot.create(arrowSchema, nativeWriter.getAllocator());
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);


        nativeWriter.withPrefix(this.prefix);
        nativeWriter.useDynamicPartition(true);

        FlinkUtil.setFSConfigs(conf, nativeWriter);
        nativeWriter.initializeWriter();
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        this.totalRows++;
        if (this.rowsInBatch >= this.batchSize) {
            this.arrowWriter.finish();
            this.nativeWriter.write(this.batch);
            // in native writer, batch may be kept in memory for sorting,
            // so we have to release ownership in java
            this.batch.clear();
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
        }
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }


    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        try {
            this.nativeWriter.close();
            initNativeWriter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new NativeParquetWriter.NativeWriterPendingFileRecoverable(this.prefix, this.creationTime);
    }

    public Map<String, List<PendingFileRecoverable>> closeForCommitWithRecoverableMap() throws IOException {
        this.arrowWriter.finish();
        Map<String, List<PendingFileRecoverable>> recoverableMap = new HashMap<>();
        if (this.batch.getRowCount() > 0) {
            this.nativeWriter.write(this.batch);
            HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
//        System.out.println(partitionDescAndFilesMap);
            for (Map.Entry<String, List<String>> entry : partitionDescAndFilesMap.entrySet()) {
                recoverableMap.put(
                        entry.getKey(),
                        entry.getValue()
                                .stream()
                                .map(path -> new NativeParquetWriter.NativeWriterPendingFileRecoverable(path, creationTime))
                                .collect(Collectors.toList())
                );
            }
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
            this.batch.clear();
            this.batch.close();
            try {
                this.nativeWriter.close();
                initNativeWriter();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return recoverableMap;
    }

    @Override
    public void dispose() {
        try {
            this.arrowWriter.finish();
            this.batch.close();
            this.nativeWriter.close();
            this.nativeWriter = null;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getBucketId() {
        return DYNAMIC_BUCKET;
    }

    @Override
    public long getCreationTime() {
        return this.creationTime;
    }

    @Override
    public long getSize() throws IOException {
        return totalRows;
    }

    @Override
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
}
