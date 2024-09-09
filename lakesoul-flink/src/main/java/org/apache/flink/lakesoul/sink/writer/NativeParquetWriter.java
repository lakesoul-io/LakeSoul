// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.tool.NativeOptions;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class NativeParquetWriter implements InProgressFileWriter<RowData, String> {

    private static final Logger LOG = LoggerFactory.getLogger(NativeParquetWriter.class);

    private final RowType rowType;
    private final int subTaskId;
    private ArrowWriter<RowData> arrowWriter;
    private final List<String> primaryKeys;
    private final List<String> rangeColumns;
    private NativeIOWriter nativeWriter;
    private final Configuration conf;
    private final int maxRowGroupRows;

    private final long creationTime;

    private VectorSchemaRoot batch;

    private final String bucketID;

    private int rowsInBatch;

    long lastUpdateTime;

    Path prefix;

    private long totalRows = 0;
    private final boolean isDynamicBucket;

    public NativeParquetWriter(RowType rowType,
                               List<String> primaryKeys,
                               List<String> rangeColumns,
                               String bucketID,
                               Path path,
                               long creationTime,
                               Configuration conf,
                               int subTaskId) throws IOException {
        this.maxRowGroupRows = conf.getInteger(MAX_ROW_GROUP_SIZE);
        this.creationTime = creationTime;
        this.bucketID = bucketID;
        this.isDynamicBucket = DYNAMIC_BUCKET.equals(bucketID);
        this.rowsInBatch = 0;
        this.rowType = rowType;
        this.primaryKeys = primaryKeys;
        this.rangeColumns = rangeColumns;
        this.conf = conf;
        this.subTaskId = subTaskId;

        this.prefix = path.makeQualified(path.getFileSystem());
        if (!bucketID.isEmpty() && !isDynamicBucket) {
            this.prefix = new Path(this.prefix, bucketID);
        }
        initNativeWriter();

    }


    private void initNativeWriter() throws IOException {
        ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema);
        nativeWriter.setPrimaryKeys(primaryKeys);

        if (conf.getBoolean(LakeSoulSinkOptions.isMultiTableSource)) {
            nativeWriter.setAuxSortColumns(Collections.singletonList(SORT_FIELD));
        }
        nativeWriter.setHashBucketNum(conf.getInteger(LakeSoulSinkOptions.HASH_BUCKET_NUM));

        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);
        batch = VectorSchemaRoot.create(arrowSchema, nativeWriter.getAllocator());
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);

        nativeWriter.withPrefix(this.prefix.toString());
        nativeWriter.setOption(NativeOptions.HASH_BUCKET_ID.key(), String.valueOf(subTaskId));
        if (isDynamicBucket) {
            nativeWriter.setRangePartitions(rangeColumns);
            nativeWriter.useDynamicPartition(true);
        }

        FlinkUtil.setIOConfigs(conf, nativeWriter);
        nativeWriter.initializeWriter();
        LOG.info("Initialized NativeParquetWriter: {}", this);
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        this.totalRows++;
        if (this.rowsInBatch >= this.maxRowGroupRows) {
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

    public static class NativePendingFileRecoverableSerializer
            implements SimpleVersionedSerializer<PendingFileRecoverable> {

        public static final NativePendingFileRecoverableSerializer INSTANCE =
                new NativePendingFileRecoverableSerializer();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(InProgressFileWriter.PendingFileRecoverable obj) throws IOException {
            if (!(obj instanceof NativeParquetWriter.NativeWriterPendingFileRecoverable)) {
                throw new UnsupportedOperationException(
                        "Only NativeParquetWriter.NativeWriterPendingFileRecoverable is supported.");
            }
            DataOutputSerializer out = new DataOutputSerializer(256);
            NativeParquetWriter.NativeWriterPendingFileRecoverable recoverable =
                    (NativeParquetWriter.NativeWriterPendingFileRecoverable) obj;
            out.writeUTF(recoverable.path);
            out.writeLong(recoverable.creationTime);
            return out.getCopyOfBuffer();
        }

        @Override
        public InProgressFileWriter.PendingFileRecoverable deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            String path = in.readUTF();
            long time = in.readLong();
            return new NativeParquetWriter.NativeWriterPendingFileRecoverable(path, time);
        }
    }

    static public class NativeWriterPendingFileRecoverable implements PendingFileRecoverable, Serializable {
        public String path;

        public long creationTime;

        public NativeWriterPendingFileRecoverable(String path, long creationTime) {
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public String toString() {
            return "PendingFile(" +
                    path + ", " + creationTime + ")";
        }

        @Nullable
        @Override
        public Path getPath() {
            return new Path(path);
        }

        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, creationTime);
        }
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        try {
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new NativeWriterPendingFileRecoverable(this.prefix.toString(), this.creationTime);
    }

    public Map<String, List<PendingFileRecoverable>> closeForCommitWithRecoverableMap() throws IOException {
        long timer = System.currentTimeMillis();
        this.arrowWriter.finish();
        Map<String, List<PendingFileRecoverable>> recoverableMap = new HashMap<>();
        if (this.batch.getRowCount() > 0) {
            this.nativeWriter.write(this.batch);
        }
        HashMap<String, List<String>> partitionDescAndFilesMap = this.nativeWriter.flush();
        for (Map.Entry<String, List<String>> entry : partitionDescAndFilesMap.entrySet()) {
            String key = isDynamicBucket ? entry.getKey() : bucketID;
            recoverableMap.put(
                    key,
                    entry.getValue()
                            .stream()
                            .map(path -> new NativeParquetWriter.NativeWriterPendingFileRecoverable(path,
                                    creationTime))
                            .collect(Collectors.toList())
            );
        }
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        try {
            this.nativeWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.info("CloseForCommitWithRecoverableMap done, costTime={}ms, recoverableMap={}", System.currentTimeMillis() - timer, recoverableMap);
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
        return this.bucketID;
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
        return "NativeParquetWriter{" +
                "maxRowGroupRows=" + maxRowGroupRows +
                ", creationTime=" + creationTime +
                ", bucketID='" + bucketID + '\'' +
                ", rowsInBatch=" + rowsInBatch +
                ", lastUpdateTime=" + lastUpdateTime +
                ", path=" + prefix +
                ", totalRows=" + totalRows +
                '}';
    }
}
