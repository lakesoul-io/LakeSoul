// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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

    private long tsMs;

    private final List<InProgressFileWriter.PendingFileRecoverable> pendingFiles =
            new ArrayList<>();

    private long partCounter;

    @Nullable
    private InProgressFileWriter<RowData, String> inProgressPartWriter;

    private final TableSchemaIdentity tableId;

    /**
     * Constructor to create a new empty bucket.
     */
    private LakeSoulWriterBucket(
            int subTaskId, TableSchemaIdentity tableId,
            String bucketId,
            Path bucketPath,
            BucketWriter<RowData, String> bucketWriter,
            RollingPolicy<RowData, String> rollingPolicy,
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
    }

    /**
     * Constructor to restore a bucket from checkpointed state.
     */
    private LakeSoulWriterBucket(
            int subTaskId,
            TableSchemaIdentity tableId,
            BucketWriter<RowData, String> partFileFactory,
            RollingPolicy<RowData, String> rollingPolicy,
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
        pendingFiles.addAll(state.getPendingFileRecoverableList());
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
        return inProgressPartWriter != null || pendingFiles.size() > 0;
    }

    void merge(final LakeSoulWriterBucket bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    void write(RowData element, long currentTime, long tsMs) throws IOException {
        if (inProgressPartWriter == null || rollingPolicy.shouldRollOnEvent(inProgressPartWriter, element)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Opening new part file for bucket id={} due to element {}.",
                        bucketId,
                        element);
            }
            inProgressPartWriter = rollPartFile(currentTime);
            this.tsMs = tsMs;
        }

        inProgressPartWriter.write(element, currentTime);
    }

    List<LakeSoulMultiTableSinkCommittable> prepareCommit(boolean flush,String dmlType) throws IOException {
        // we always close part file and do not keep in-progress file
        // since the native parquet writer doesn't support resume
        if (inProgressPartWriter != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Closing in-progress part file for bucket id={} on checkpoint.", bucketId);
            }
            closePartFile();
        }

        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        long time = pendingFiles.isEmpty() ? Long.MIN_VALUE :
                ((NativeParquetWriter.NativeWriterPendingFileRecoverable) pendingFiles.get(0)).creationTime;

        // this.pendingFiles would be cleared later, we need to make a copy
        List<InProgressFileWriter.PendingFileRecoverable> tmpPending = new ArrayList<>(pendingFiles);
        committables.add(new LakeSoulMultiTableSinkCommittable(
                bucketId,
                tmpPending,
                time, tableId, tsMs,dmlType));
        pendingFiles.clear();

        return committables;
    }

    LakeSoulWriterBucketState snapshotState() throws IOException {
        if (inProgressPartWriter != null) {
            closePartFile();
        }

        // this.pendingFiles would be cleared later, we need to make a copy
        List<InProgressFileWriter.PendingFileRecoverable> tmpPending = new ArrayList<>(pendingFiles);
        return new LakeSoulWriterBucketState(
                tableId,
                bucketId,
                bucketPath,
                tmpPending);
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPartWriter != null
                && rollingPolicy.shouldRollOnProcessingTime(inProgressPartWriter, timestamp)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Bucket {} closing in-progress part file for part file id={} due to processing time rolling " +
                                "policy "
                                + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                        bucketId,
                        uniqueId,
                        inProgressPartWriter.getCreationTime(),
                        inProgressPartWriter.getLastUpdateTime(),
                        timestamp);
            }

            closePartFile();
        }
    }

    private InProgressFileWriter<RowData, String> rollPartFile(long currentTime) throws IOException {
        closePartFile();

        final Path partFilePath = assembleNewPartPath();

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Opening new part file \"{}\" for bucket id={}.",
                    partFilePath.getName(),
                    bucketId);
        }

        return bucketWriter.openNewInProgressFile(bucketId, partFilePath, currentTime);
    }

    /**
     * Constructor a new PartPath and increment the partCounter.
     */
    private Path assembleNewPartPath() {
        long currentPartCounter = partCounter++;
        String count = String.format("%03d", currentPartCounter);
        String subTask = String.format("%05d", this.subTaskId);
        return new Path(
                bucketPath,
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
        if (inProgressPartWriter != null) {
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                    inProgressPartWriter.closeForCommit();
            pendingFiles.add(pendingFileRecoverable);
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
            final BucketWriter<RowData, String> bucketWriter,
            final RollingPolicy<RowData, String> rollingPolicy,
            final OutputFileConfig outputFileConfig) {
        return new LakeSoulWriterBucket(
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
    static LakeSoulWriterBucket restore(
            int subTaskId,
            final TableSchemaIdentity tableId,
            final BucketWriter<RowData, String> bucketWriter,
            final RollingPolicy<RowData, String> rollingPolicy,
            final LakeSoulWriterBucketState bucketState,
            final OutputFileConfig outputFileConfig) throws IOException {
        return new LakeSoulWriterBucket(subTaskId, tableId, bucketWriter, rollingPolicy, bucketState, outputFileConfig);
    }
}
