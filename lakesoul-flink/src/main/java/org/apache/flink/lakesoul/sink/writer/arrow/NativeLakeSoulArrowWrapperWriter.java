// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.writer.NativeParquetWriter;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.MAX_ROW_GROUP_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SORT_FIELD;

public class NativeLakeSoulArrowWrapperWriter implements InProgressFileWriter<LakeSoulArrowWrapper, String> {

    private static final Logger LOG = LoggerFactory.getLogger(NativeLakeSoulArrowWrapperWriter.class);

    private final RowType rowType;

    private final List<String> primaryKeys;

    private final List<String> rangeColumns;

    private final Configuration conf;

    private NativeIOWriter nativeWriter;

    private final int maxRowGroupRows;

    private final long creationTime;

    long lastUpdateTime;

    String prefix;

    private long totalRows = 0;

    public NativeLakeSoulArrowWrapperWriter(RowType rowType,
                                            List<String> primaryKeys,
                                            List<String> rangeColumns,
                                            Path path,
                                            long creationTime,
                                            Configuration conf) throws IOException {
        this.maxRowGroupRows = conf.getInteger(MAX_ROW_GROUP_SIZE);
        this.creationTime = creationTime;
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

        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);

        nativeWriter.withPrefix(this.prefix);
        nativeWriter.useDynamicPartition(true);

        FlinkUtil.setFSConfigs(conf, nativeWriter);
        nativeWriter.initializeWriter();
        LOG.info("Initialized NativeLakeSoulArrowWrapperWriter: {}", this);
    }

    @Override
    public void write(LakeSoulArrowWrapper element, long currentTime) throws IOException {
        totalRows += nativeWriter.writeIpc(element.getEncodedBatch());
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }


    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
        try {
            this.nativeWriter.close();
            initNativeWriter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new NativeParquetWriter.NativeWriterPendingFileRecoverable(this.prefix, this.creationTime);
    }

    public Map<String, List<PendingFileRecoverable>> closeForCommitWithRecoverableMap() throws IOException {
        Map<String, List<PendingFileRecoverable>> recoverableMap = new HashMap<>();

        HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
        for (Map.Entry<String, List<String>> entry : partitionDescAndFilesMap.entrySet()) {
            recoverableMap.put(
                    entry.getKey(),
                    entry.getValue()
                            .stream()
                            .map(path -> new NativeParquetWriter.NativeWriterPendingFileRecoverable(path, creationTime))
                            .collect(Collectors.toList())
            );
        }

        try {
            this.nativeWriter.close();
            initNativeWriter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LOG.info("CloseForCommitWithRecoverableMap done, recoverableMap={}", recoverableMap);
        return recoverableMap;
    }

    @Override
    public void dispose() {
        try {
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

    @Override
    public String toString() {
        return "NativeLakeSoulArrowWrapperWriter{" +
                "rowType=" + rowType +
                ", primaryKeys=" + primaryKeys +
                ", rangeColumns=" + rangeColumns +
                ", maxRowGroupRows=" + maxRowGroupRows +
                ", creationTime=" + creationTime +
                ", prefix='" + prefix + '\'' +
                '}';
    }
}
