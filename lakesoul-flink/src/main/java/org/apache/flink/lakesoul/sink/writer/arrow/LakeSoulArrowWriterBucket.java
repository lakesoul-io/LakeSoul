// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.writer.NativeParquetWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

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
public class LakeSoulArrowWriterBucket {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulArrowWriterBucket.class);

    private final int subTaskId;

    private final String bucketId;

    private final Path bucketPath;

    private final BucketWriter<LakeSoulArrowWrapper, String> bucketWriter;

    private final RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy;

    private final OutputFileConfig outputFileConfig;

    private final String uniqueId;

    private long tsMs;

    private final Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFilesMap =
            new HashMap<>();

    private long partCounter;

    private int restartTimes;

    @Nullable
    private InProgressFileWriter<LakeSoulArrowWrapper, String> inProgressPartWriter;

    private final TableSchemaIdentity tableId;

    /**
     * Constructor to create a new empty bucket.
     */
    public LakeSoulArrowWriterBucket(
            int subTaskId, TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<LakeSoulArrowWrapper, String> bucketWriter,
            RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            OutputFileConfig outputFileConfig) {
        this.subTaskId = subTaskId;
        this.tableId = checkNotNull(tableId);
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
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
    public LakeSoulArrowWriterBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<LakeSoulArrowWrapper, String> partFileFactory,
            RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            LakeSoulWriterBucketState bucketState,
            OutputFileConfig outputFileConfig) throws IOException {

        this(
                subTaskId, tableId,
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
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

    void merge(final LakeSoulArrowWriterBucket bucket) throws IOException {
        checkNotNull(bucket);

        bucket.closePartFile();
        for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : bucket.pendingFilesMap.entrySet()) {
            pendingFilesMap.computeIfAbsent(entry.getKey(), key -> new ArrayList<>()).addAll(entry.getValue());
        }

        LOG.info("Merging buckets for bucket id={}", getBucketId());
    }

    void write(LakeSoulArrowWrapper element, long currentTime, long tsMs) throws IOException {
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
            closePartFile();
            LOG.info(
                    "Closing in-progress part file for flush={} bucket id={} subTaskId={} tableId={} pendingFilesMap={} on checkpoint.",
                    flush,
                    getBucketId(),
                    subTaskId,
                    tableId,
                    pendingFilesMap);

        }

        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        if (!pendingFilesMap.isEmpty()) {
            long time = ((NativeParquetWriter.NativeWriterPendingFileRecoverable) pendingFilesMap.values().stream().findFirst()
                    .get().get(0)).creationTime;

            committables.add(new LakeSoulMultiTableSinkCommittable(
                    tableId,
                    new HashMap<>(pendingFilesMap),
                    time,
                    UUID.randomUUID().toString(),
                    tsMs,
                    dmlType,
                    sourcePartitionInfo
            ));
            LOG.info("prepareCommit {}", committables);
            pendingFilesMap.clear();
        }

        return committables;
    }

    LakeSoulWriterBucketState snapshotState() throws IOException {
        if (inProgressPartWriter != null) {
            closePartFile();
        }

        HashMap<String, List<InProgressFileWriter.PendingFileRecoverable>> tmpPending = new HashMap<>(pendingFilesMap);
        return new LakeSoulWriterBucketState(tableId, bucketPath, tmpPending);
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

    private InProgressFileWriter<LakeSoulArrowWrapper, String> rollPartFile(long currentTime) throws IOException {
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
        if (DYNAMIC_BUCKET.equals(bucketId)) {
            return bucketPath;
        }
        long currentPartCounter = partCounter++;
        String count = String.format("%03d", currentPartCounter);
        String subTask = String.format("%05d", this.subTaskId);
        return new Path(
                assembleBucketPath(bucketPath, bucketId),
                outputFileConfig.getPartPrefix()
                        + '-'
                        + subTask
                        + '-'
                        + uniqueId
                        + '_'
                        + subTask
                        + ".c"
                        + count
                        + outputFileConfig.getPartSuffix());
    }

    private void closePartFile() throws IOException {
//        LOG.info("ClosePartFile {}", inProgressPartWriter);
        if (inProgressPartWriter != null) {
            if (inProgressPartWriter instanceof NativeLakeSoulArrowWrapperWriter) {
                Map<String, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverableMap =
                        ((NativeLakeSoulArrowWrapperWriter) inProgressPartWriter).closeForCommitWithRecoverableMap();
                for (Map.Entry<String, List<InProgressFileWriter.PendingFileRecoverable>> entry : pendingFileRecoverableMap.entrySet()) {
                    pendingFilesMap.computeIfAbsent(entry.getKey(), bucketId -> new ArrayList())
                            .addAll(entry.getValue());
                }
                inProgressPartWriter = null;
            } else {
                throw new RuntimeException(
                        "inProgressPartWriter only support instanceof NativeLakeSoulArrowWrapperWriter");
            }
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
    static LakeSoulArrowWriterBucket getNew(
            int subTaskId,
            final TableSchemaIdentity tableId,
            final String bucketId,
            final Path bucketPath,
            final BucketWriter<LakeSoulArrowWrapper, String> bucketWriter,
            final RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            final OutputFileConfig outputFileConfig) {
        return new LakeSoulArrowWriterBucket(
                subTaskId, tableId,
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
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
    static LakeSoulArrowWriterBucket restore(
            int subTaskId,
            final TableSchemaIdentity tableId,
            final BucketWriter<LakeSoulArrowWrapper, String> bucketWriter,
            final RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
            final LakeSoulWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig) throws IOException {
        return new LakeSoulArrowWriterBucket(subTaskId, tableId, bucketWriter, rollingPolicy, bucketState,
                outputFileConfig);
    }
}
