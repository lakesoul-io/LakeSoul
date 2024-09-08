// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.meta.entity.JniWrapper;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A bucket is the directory organization of the output of the {@link LakeSoulMultiTablesSink}.
 *
 * <p>For each incoming element in the {@code LakeSoulMultiTablesSink}, the user-specified {@link BucketAssigner}
 * is queried to see in which bucket this element should be written to.
 *
 * <p>This writer is responsible for writing the input data and creating pending (uncommitted) files.
 */
public class LakeSoulWriterBucket {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulWriterBucket.class);

    private final int subTaskId;

    private final String bucketId;

    private final Path bucketPath;

    private final BucketWriter<RowData, String> bucketWriter;

    private final RollingPolicy<RowData, String> rollingPolicy;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;
    private final Configuration conf;

    private long tsMs;

    private int restartTimes;

    private final Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFilesMap =
            new HashMap<>();

    private long partCounter;

    @Nullable
    private InProgressFileWriter<RowData, String> inProgressPartWriter;

    private final TableSchemaIdentity tableId;

    /**
     * Constructor to create a new empty bucket.
     */
    private LakeSoulWriterBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            Configuration conf,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {
        this.subTaskId = subTaskId;
        this.tableId = checkNotNull(tableId);
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.conf = checkNotNull(conf);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.uniqueId = UUID.randomUUID().toString();
        this.partCounter = 0;
        this.restartTimes = 0;
    }

    /**
     * Constructor to restore a bucket from checkpointed state.
     */
    private LakeSoulWriterBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            Configuration conf,
            BucketWriter<RowData, String> partFileFactory,
            RollingPolicy<RowData, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig) throws IOException {

        this(
                subTaskId, tableId,
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
                conf,
                partFileFactory,
                rollingPolicy,
                outputFileConfig);

        restoreState(bucketState);
    }

    private void restoreState(LakeSoulWriterBucketState state) throws IOException {
        for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : state.getPendingFileRecoverableMap()
                .entrySet()) {
            pendingFilesMap.computeIfAbsent(entry.getKey(), key -> new ArrayList<>()).addAll(entry.getValue());
        }
        restartTimes = state.getRestartTimes();
    }

    public String getBucketId() {
        return bucketId;
    }

    public Path getBucketPath() {
        return bucketPath;
    }

    public long getPartCounter() {
        return partCounter;
    }

    public boolean isActive() {
        return inProgressPartWriter != null || !pendingFilesMap.isEmpty();
    }

    void merge(final LakeSoulWriterBucket bucket) throws IOException {
        checkNotNull(bucket);

        bucket.closePartFile();
        for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : bucket.pendingFilesMap.entrySet()) {
            pendingFilesMap.computeIfAbsent(entry.getKey(), key -> new ArrayList<>()).addAll(entry.getValue());
        }

        LOG.info("Merging buckets for bucket id={}", getBucketId());
    }

    void write(RowData element, long currentTime, long tsMs) throws IOException {
        if (inProgressPartWriter == null || rollingPolicy.shouldRollOnEvent(inProgressPartWriter, element)) {
            LOG.info(
                    "Opening new part file for bucket id={} at {}.",
                    getBucketId(),
                    tsMs);
            inProgressPartWriter = rollPartFile(currentTime);
            this.tsMs = tsMs;
        }

        inProgressPartWriter.write(element, currentTime);
    }

    List<LakeSoulMultiTableSinkCommittable> prepareCommit(boolean flush, String dmlType, String sourcePartitionInfo)
            throws IOException {
        // we always close part file and do not keep in-progress file
        // since the native parquet writer doesn't support resume
        if (inProgressPartWriter != null) {
            LOG.info(
                    "Closing in-progress part file for bucket id={} on checkpoint.", getBucketId());
            closePartFile();
        }

        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        long time = pendingFilesMap.isEmpty() ? Long.MIN_VALUE :
                ((NativeParquetWriter.NativeWriterPendingFileRecoverable) pendingFilesMap.values().stream().findFirst()
                        .get().get(0)).creationTime;

        if (dmlType.equals(LakeSoulSinkOptions.DELETE)) {
            List<PartitionInfo> sourcePartitionInfoList = JniWrapper
                    .parseFrom(Base64.getDecoder().decode(sourcePartitionInfo))
                    .getPartitionInfoList();

            for (PartitionInfo partitionInfo : sourcePartitionInfoList) {
                String partitionDesc = partitionInfo.getPartitionDesc();
                pendingFilesMap.computeIfAbsent(partitionDesc, _partitionDesc -> new ArrayList());
            }
        }
        committables.add(new LakeSoulMultiTableSinkCommittable(
                tableId,
                new HashMap<>(pendingFilesMap),
                time,
                UUID.randomUUID().toString(),
                tsMs,
                dmlType,
                sourcePartitionInfo
        ));
        pendingFilesMap.clear();

        return committables;
    }

    LakeSoulWriterBucketState snapshotState() throws IOException {
        if (inProgressPartWriter != null) {
            closePartFile();
        }

        return new LakeSoulWriterBucketState(tableId, bucketPath, new HashMap<>(pendingFilesMap));
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPartWriter != null
                && rollingPolicy.shouldRollOnProcessingTime(inProgressPartWriter, timestamp)) {
            LOG.info(
                    "Bucket {} closing in-progress part file for part file id={} due to processing time rolling " +
                            "policy "
                            + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                    getBucketId(),
                    uniqueId,
                    inProgressPartWriter.getCreationTime(),
                    inProgressPartWriter.getLastUpdateTime(),
                    timestamp);

            closePartFile();
        }
    }

    private InProgressFileWriter<RowData, String> rollPartFile(long currentTime) throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        LOG.info(
                "Opening new part file \"{}\" for bucket id={}.",
                partFilePath.getName(),
                getBucketId());

        return bucketWriter.openNewInProgressFile(getBucketId(), partFilePath, currentTime);
    }

    private Path assembleBucketPath(Path basePath, String bucketId) {
        if ("".equals(bucketId)) {
            return basePath;
        }
        return new Path(basePath, bucketId);
    }

    /**
     * Constructor a new PartPath and increment the partCounter.
     */
    private Path assembleNewPartPath() {
        return bucketPath;
    }

    private void closePartFile() throws IOException {
        if (inProgressPartWriter != null) {
            long start = System.currentTimeMillis();
            Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableMap =
                    ((NativeParquetWriter) inProgressPartWriter).closeForCommitWithRecoverableMap();
            for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : pendingFileRecoverableMap.entrySet()) {
                pendingFilesMap.computeIfAbsent(entry.getKey(), bucketId -> new ArrayList())
                        .addAll(entry.getValue());
            }
            inProgressPartWriter = null;
        }
    }

    void disposePartFile() {
        if (inProgressPartWriter != null) {
            inProgressPartWriter.dispose();
            inProgressPartWriter = null;
        }
    }

    // --------------------------- Static Factory Methods -----------------------------

    /**
     * Creates a new empty {@code Bucket}.
     *
     * @param bucketId         the identifier of the bucket, as returned by the {@link BucketAssigner}.
     * @param bucketPath       the path to where the part files for the bucket will be written to.
     * @param bucketWriter     the {@link BucketWriter} used to write part files in the bucket.
     * @param outputFileConfig the part file configuration.
     * @return The new Bucket.
     */
    static LakeSoulWriterBucket getNew(
            int subTaskId,
            final TableSchemaIdentity tableId,
            final String bucketId,
            final Path bucketPath,
            final Configuration conf,
            final BucketWriter<RowData, String> bucketWriter,
            final RollingPolicy<RowData, String> rollingPolicy,
            final OutputFileConfig outputFileConfig) {
        return new LakeSoulWriterBucket(
                subTaskId, tableId,
                bucketId, bucketPath, conf, bucketWriter, rollingPolicy, outputFileConfig);
    }

    /**
     * Restores a {@code Bucket} from the state included in the provided {@link
     * LakeSoulWriterBucketState}.
     *
     * @param bucketWriter     the {@link BucketWriter} used to write part files in the bucket.
     * @param bucketState      the initial state of the restored bucket.
     * @param outputFileConfig the part file configuration.
     * @return The restored Bucket.
     */
    static LakeSoulWriterBucket restore(
            int subTaskId,
            final TableSchemaIdentity tableId,
            final BucketWriter<RowData, String> bucketWriter,
            final Configuration conf,
            final RollingPolicy<RowData, String> rollingPolicy,
            final LakeSoulWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig) throws IOException {
        return new LakeSoulWriterBucket(subTaskId, tableId, conf, bucketWriter, rollingPolicy, bucketState,
                outputFileConfig);
    }
}
