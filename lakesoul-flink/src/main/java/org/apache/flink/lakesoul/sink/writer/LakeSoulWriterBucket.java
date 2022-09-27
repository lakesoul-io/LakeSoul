/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.types.LakeSoulCDCComparator;
import org.apache.flink.lakesoul.types.LakeSoulCDCElement;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A bucket is the directory organization of the output of the {@link LakeSoulMultiTablesSink}.
 *
 * <p>For each incoming element in the {@code LakeSoulMultiTablesSink}, the user-specified {@link BucketAssigner}
 * is queried to see in which bucket this element should be written to.
 *
 * <p>This writer is responsible for writing the input data and managing the staging area used by a
 * bucket to temporarily store in-progress, uncommitted data.
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

    private final List<PendingFileAndCreateTime> pendingFiles =
            new ArrayList<>();

    private final List<String> filePaths = new ArrayList<>();

    private long partCounter;

    @Nullable
    private InProgressFileRecoverable inProgressFileToCleanup;

    @Nullable
    private InProgressFileWriter<RowData, String> inProgressPart;

    private String inProgressPath;

    private final PriorityQueue<LakeSoulCDCElement> sortQueue;

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
            OutputFileConfig outputFileConfig,
            LakeSoulCDCComparator comparator) {
        this.subTaskId = subTaskId;
        this.tableId = checkNotNull(tableId);
        this.bucketId = checkNotNull(bucketId);
        this.bucketPath = checkNotNull(bucketPath);
        this.bucketWriter = checkNotNull(bucketWriter);
        this.rollingPolicy = checkNotNull(rollingPolicy);
        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.uniqueId = UUID.randomUUID().toString();
        this.partCounter = 0;
        this.sortQueue = new PriorityQueue<>(comparator);
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
            OutputFileConfig outputFileConfig,
            LakeSoulCDCComparator comparator)
            throws IOException {

        this(
                subTaskId, tableId,
                bucketState.getBucketId(),
                bucketState.getBucketPath(),
                partFileFactory,
                rollingPolicy,
                outputFileConfig,
                comparator);

        restoreInProgressFile(bucketState);
    }

    private void restoreInProgressFile(LakeSoulWriterBucketState state) throws IOException {
        if (!state.hasInProgressFileRecoverable()) {
            return;
        }

        // we try to resume the previous in-progress file
        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable =
                state.getInProgressFileRecoverable();
        String path = state.getInProgressPath();

        if (bucketWriter.getProperties().supportsResume()) {
            inProgressPart =
                    bucketWriter.resumeInProgressFileFrom(
                            bucketId,
                            inProgressFileRecoverable,
                            state.getInProgressFileCreationTime());
            this.inProgressPath = path;
        } else {
            pendingFiles.add(new PendingFileAndCreateTime(inProgressFileRecoverable,
                                                          state.getInProgressFileCreationTime()));
            filePaths.add(path);
        }
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
        return inProgressPart != null || inProgressFileToCleanup != null || pendingFiles.size() > 0;
    }

    void merge(final LakeSoulWriterBucket bucket) throws IOException {
        checkNotNull(bucket);
        checkState(Objects.equals(bucket.bucketPath, bucketPath));

        bucket.closePartFile();
        pendingFiles.addAll(bucket.pendingFiles);
        filePaths.addAll(bucket.filePaths);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Merging buckets for bucket id={}", bucketId);
        }
    }

    void write(RowData element, long currentTime) throws IOException {
        if (inProgressPart == null || rollingPolicy.shouldRollOnEvent(inProgressPart, element)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Opening new part file for bucket id={} due to element {}.",
                        bucketId,
                        element);
            }
            inProgressPart = rollPartFile(currentTime);
        }

        sortQueue.add(new LakeSoulCDCElement(element, currentTime));
    }

    List<LakeSoulMultiTableSinkCommittable> prepareCommit(boolean flush) throws IOException {
        if (inProgressPart != null
            && (rollingPolicy.shouldRollOnCheckpoint(inProgressPart) || flush)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Closing in-progress part file for bucket id={} on checkpoint.", bucketId);
            }
            closePartFile();
        }

        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        long time = pendingFiles.isEmpty() ? Long.MIN_VALUE : pendingFiles.get(0).inProgressFileCreationTime;
        committables.add(new LakeSoulMultiTableSinkCommittable(
                bucketId,
                pendingFiles.stream().map(p -> p.pendingFile).collect(Collectors.toList()),
                new ArrayList<>(filePaths), time, tableId));
        pendingFiles.clear();
        filePaths.clear();

        if (inProgressFileToCleanup != null) {
            committables.add(new LakeSoulMultiTableSinkCommittable(bucketId, tableId, inProgressFileToCleanup));
            inProgressFileToCleanup = null;
        }

        return committables;
    }

    LakeSoulWriterBucketState snapshotState() throws IOException {
        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
        long inProgressFileCreationTime = Long.MAX_VALUE;

        if (inProgressPart != null) {
            inProgressFileRecoverable = inProgressPart.persist();
            inProgressFileToCleanup = inProgressFileRecoverable;
            inProgressFileCreationTime = inProgressPart.getCreationTime();
        }

        return new LakeSoulWriterBucketState(
                tableId,
                bucketId, bucketPath, inProgressFileCreationTime, inProgressFileRecoverable, inProgressPath);
    }

    void onProcessingTime(long timestamp) throws IOException {
        if (inProgressPart != null
            && rollingPolicy.shouldRollOnProcessingTime(inProgressPart, timestamp)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Bucket {} closing in-progress part file for part file id={} due to processing time rolling " +
                        "policy "
                        + "(in-progress file created @ {}, last updated @ {} and current time is {}).",
                        bucketId,
                        uniqueId,
                        inProgressPart.getCreationTime(),
                        inProgressPart.getLastUpdateTime(),
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

        inProgressPath = partFilePath.toString();
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
                + this.subTaskId
                + ".c"
                + count
                + outputFileConfig.getPartSuffix());
    }

    private void closePartFile() throws IOException {
        if (inProgressPart != null) {
            long creationTime = inProgressPart.getCreationTime();
            while (!sortQueue.isEmpty()) {
                LakeSoulCDCElement element = sortQueue.poll();
                inProgressPart.write(element.element, element.timedata);
            }
            InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable =
                    inProgressPart.closeForCommit();
            pendingFiles.add(new PendingFileAndCreateTime(pendingFileRecoverable, creationTime));
            filePaths.add(inProgressPath);
            inProgressPart = null;
            inProgressPath = null;
        }
    }

    void disposePartFile() {
        if (inProgressPart != null) {
            inProgressPart.dispose();
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
            final OutputFileConfig outputFileConfig,
            final LakeSoulCDCComparator comparator) {
        return new LakeSoulWriterBucket(
                subTaskId, tableId,
                bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig, comparator);
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
            final OutputFileConfig outputFileConfig,
            final LakeSoulCDCComparator comparator) throws IOException {
        return new LakeSoulWriterBucket(subTaskId, tableId, bucketWriter, rollingPolicy, bucketState, outputFileConfig,
                                        comparator);
    }
}
