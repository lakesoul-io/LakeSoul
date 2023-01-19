package org.apache.flink.lakesoul.sink.writer;

import org.apache.arrow.lakesoul.io.NativeIOWriter;
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;

public class NativeParquetWriter implements InProgressFileWriter<RowData, String> {
    private final ArrowWriter<RowData> arrowWriter;

    private final NativeIOWriter nativeWriter;

    private final int batchSize;

    private final long creationTime;

    private final BufferAllocator allocator;

    private final VectorSchemaRoot batch;

    private final String bucketID;

    private int rowsInBatch;

    long lastUpdateTime;

    String path;

    public NativeParquetWriter(RowType rowType, int batchSize, String bucketID, Path path, long creationTime, Configuration conf) throws IOException {
        this.batchSize = batchSize;
        this.creationTime = creationTime;
        this.bucketID = bucketID;
        this.rowsInBatch = 0;

        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema.toJson());
        allocator = ArrowMemoryUtils.rootAllocator.newChildAllocator("FlinkNativeWriter", 0, Long.MAX_VALUE);
        batch = VectorSchemaRoot.create(arrowSchema, allocator);
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);
        this.path = path.makeQualified(path.getFileSystem()).toString();
        nativeWriter.addFile(this.path);

        setFSConfigs(conf);
        this.nativeWriter.initializeWriter();
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        if (this.rowsInBatch >= this.batchSize) {
            this.arrowWriter.finish();
            this.nativeWriter.write(this.batch);
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
        }
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }

    static public class NativeWriterPendingFileRecoverable implements PendingFileRecoverable, Serializable {
        public String path;

        NativeWriterPendingFileRecoverable(String path) {
            this.path = path;
        }
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        return new NativeWriterPendingFileRecoverable(this.path);
    }

    @Override
    public void dispose() {
        try {
            this.nativeWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.batch.close();
        this.allocator.close();
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
        return 0;
    }

    @Override
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }

    private void setFSConfigs(Configuration conf) {
        setFSConf(conf, "fs.s3a.access.key");
        setFSConf(conf, "fs.s3a.access.secret");
        setFSConf(conf, "fs.s3a.endpoint");
        setFSConf(conf, "fs.s3a.endpoint.region");
    }

    private void setFSConf(Configuration conf, String confKey) {
        String value = conf.getString(confKey, "");
        if (!value.isEmpty()) {
            this.nativeWriter.setObjectStoreOption(confKey, value);
        }
    }
}
