// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link SinkWriter} implementation for {@link LakeSoulMultiTablesSink}.
 *
 * <p>It writes data to and manages the different active {@link LakeSoulWriterBucket buckes} in the
 * {@link LakeSoulMultiTablesSink}.
 *
 * @param <IN> The type of input elements.
 */
public abstract class AbstractLakeSoulMultiTableSinkWriter<IN>
        implements SinkWriter<IN, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState>,
        Sink.ProcessingTimeService.ProcessingTimeCallback {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLakeSoulMultiTableSinkWriter.class);

    private final int subTaskId;

    private final LakeSoulWriterBucketFactory bucketFactory;

    private final RollingPolicy<RowData, String> rollingPolicy;

    private final Sink.ProcessingTimeService processingTimeService;

    private final long bucketCheckInterval;

    // --------------------------- runtime fields -----------------------------

    private final BucketerContext bucketerContext;

    private final Map<Tuple2<TableSchemaIdentity, String>, LakeSoulWriterBucket> activeBuckets;

    private final OutputFileConfig outputFileConfig;

    private final Counter recordsOutCounter;

    private final Configuration conf;

    public AbstractLakeSoulMultiTableSinkWriter(
            int subTaskId,
            final SinkWriterMetricGroup metricGroup,
            final LakeSoulWriterBucketFactory bucketFactory,
            final RollingPolicy<RowData, String> rollingPolicy,
            final OutputFileConfig outputFileConfig,
            final Sink.ProcessingTimeService processingTimeService,
            final long bucketCheckInterval,
            final Configuration conf) {
        this.subTaskId = subTaskId;

        this.bucketFactory = checkNotNull(bucketFactory);
        this.rollingPolicy = checkNotNull(rollingPolicy);

        this.outputFileConfig = checkNotNull(outputFileConfig);

        this.activeBuckets = new HashMap<>();
        this.bucketerContext = new BucketerContext();

        this.recordsOutCounter =
                checkNotNull(metricGroup).getIOMetricGroup().getNumRecordsOutCounter();
        this.processingTimeService = checkNotNull(processingTimeService);
        checkArgument(
                bucketCheckInterval > 0,
                "Bucket checking interval for processing time should be positive.");
        this.bucketCheckInterval = bucketCheckInterval;
        this.conf = conf;
    }

    public void initializeState(List<LakeSoulWriterBucketState> bucketStates) throws IOException {
        checkNotNull(bucketStates, "The retrieved state was null.");
        LOG.info("initializeState size {}", bucketStates.size());

        for (LakeSoulWriterBucketState state : bucketStates) {
            String bucketId = state.getBucketId();

            LOG.info("initializeState restoring state: {}", state);

            TableSchemaIdentity identity = state.getIdentity();
            TableSchemaWriterCreator creator = getOrCreateTableSchemaWriterCreator(identity);

            LakeSoulWriterBucket restoredBucket =
                    bucketFactory.restoreBucket(
                            subTaskId,
                            state.getIdentity(),
                            creator.createBucketWriter(),
                            rollingPolicy,
                            state,
                            outputFileConfig);

            updateActiveBucketId(identity, bucketId, restoredBucket);
        }

        registerNextBucketInspectionTimer();
    }

    private void updateActiveBucketId(TableSchemaIdentity tableId, String bucketId, LakeSoulWriterBucket restoredBucket)
            throws IOException {
        final LakeSoulWriterBucket bucket = activeBuckets.get(Tuple2.of(tableId, bucketId));
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeBuckets.put(Tuple2.of(tableId, bucketId), restoredBucket);
        }
    }

    protected abstract TableSchemaWriterCreator getOrCreateTableSchemaWriterCreator(TableSchemaIdentity identity);

    protected abstract List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(IN element) throws Exception;

    protected long getDataDmlTsMs(IN element) {
        return Long.MAX_VALUE;
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        if (element == null) {
            return;
        }
        // setting the values in the bucketer context
        bucketerContext.update(
                context.timestamp(),
                context.currentWatermark(),
                processingTimeService.getCurrentProcessingTime());

        List<Tuple2<TableSchemaIdentity, RowData>> schemaAndRowDatas;
        long dataDmlTsMs = getDataDmlTsMs(element);
        try {
            schemaAndRowDatas = extractTableSchemaAndRowData(element);
        } catch (Exception e) {
            throw new IOException(e);
        }
        for (Tuple2<TableSchemaIdentity, RowData> schemaAndRowData : schemaAndRowDatas) {
            TableSchemaIdentity identity = schemaAndRowData.f0;
            RowData rowData = schemaAndRowData.f1;
            TableSchemaWriterCreator creator = getOrCreateTableSchemaWriterCreator(identity);
            final String bucketId = creator.bucketAssigner.getBucketId(rowData, bucketerContext);
            final LakeSoulWriterBucket bucket = getOrCreateBucketForBucketId(identity, bucketId, creator);
            bucket.write(rowData, processingTimeService.getCurrentProcessingTime(), dataDmlTsMs);
            recordsOutCounter.inc();
        }
    }

    @Override
    public List<LakeSoulMultiTableSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        String dmlType = this.conf.getString(LakeSoulSinkOptions.DMLTYPE);
        // Every time before we prepare commit, we first check and remove the inactive
        // buckets. Checking the activeness right before pre-committing avoid re-creating
        // the bucket every time if the bucket use OnCheckpointingRollingPolicy.
        Iterator<Map.Entry<Tuple2<TableSchemaIdentity, String>, LakeSoulWriterBucket>> activeBucketIt =
                activeBuckets.entrySet().iterator();
        while (activeBucketIt.hasNext()) {
            Map.Entry<Tuple2<TableSchemaIdentity, String>, LakeSoulWriterBucket> entry = activeBucketIt.next();
            if (!entry.getValue().isActive()) {
                activeBucketIt.remove();
            } else {
                committables.addAll(entry.getValue().prepareCommit(flush,dmlType));
            }
        }

        return committables;
    }

    @Override
    public List<LakeSoulWriterBucketState> snapshotState(long checkpointId) throws IOException {

        List<LakeSoulWriterBucketState> states = new ArrayList<>();
        for (LakeSoulWriterBucket bucket : activeBuckets.values()) {
            LakeSoulWriterBucketState state = bucket.snapshotState();
            LOG.info("snapshotState: {}", state);
            states.add(state);
        }

        LOG.info("snapshotState size: {}", states.size());
        return states;
    }

    private LakeSoulWriterBucket getOrCreateBucketForBucketId(
            TableSchemaIdentity identity,
            String bucketId,
            TableSchemaWriterCreator creator) throws IOException {
        LakeSoulWriterBucket bucket = activeBuckets.get(Tuple2.of(identity, bucketId));
        if (bucket == null) {
            final Path bucketPath = assembleBucketPath(creator.tableLocation, bucketId);
            BucketWriter<RowData, String> bucketWriter = creator.createBucketWriter();
            bucket =
                    bucketFactory.getNewBucket(
                            subTaskId,
                            creator.identity,
                            bucketId, bucketPath, bucketWriter, rollingPolicy, outputFileConfig);
            activeBuckets.put(Tuple2.of(identity, bucketId), bucket);
            LOG.info("Create new bucket {}, {}, {}",
                    identity, bucketId, bucketPath);
        }
        return bucket;
    }

    @Override
    public void close() {
        if (activeBuckets != null) {
            activeBuckets.values().forEach(LakeSoulWriterBucket::disposePartFile);
        }
    }

    private Path assembleBucketPath(Path basePath, String bucketId) {
        if ("".equals(bucketId)) {
            return basePath;
        }
        return new Path(basePath, bucketId);
    }

    @Override
    public void onProcessingTime(long time) throws IOException {
        for (LakeSoulWriterBucket bucket : activeBuckets.values()) {
            bucket.onProcessingTime(time);
        }

        registerNextBucketInspectionTimer();
    }

    private void registerNextBucketInspectionTimer() {
        final long nextInspectionTime =
                processingTimeService.getCurrentProcessingTime() + bucketCheckInterval;
        processingTimeService.registerProcessingTimer(nextInspectionTime, this);
    }

    /**
     * The {@link BucketAssigner.Context} exposed to the {@link BucketAssigner#getBucketId(Object,
     * BucketAssigner.Context)} whenever a new incoming element arrives.
     */
    private static final class BucketerContext implements BucketAssigner.Context {

        @Nullable
        private Long elementTimestamp;

        private long currentWatermark;

        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = Long.MIN_VALUE;
            this.currentProcessingTime = Long.MIN_VALUE;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long currentProcessingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = currentProcessingTime;
        }

        @Override
        public long currentProcessingTime() {
            return currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return elementTimestamp;
        }
    }
}
