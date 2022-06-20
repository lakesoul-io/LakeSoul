package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.lakesoul.sink.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakesoul.sink.LakesoulTableSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class LakeSoulBuckets<IN, BucketID> {
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.api.functions.sink.filesystem.Buckets.class);
    private final Path basePath;
    private final LakeSoulBucketFactory<IN, BucketID> bucketFactory;
    private final BucketAssigner<IN, BucketID> bucketAssigner;
    private final BucketWriter<IN, BucketID> bucketWriter;
    private final LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy;
    private final int subtaskIndex;
    private final BucketerContext bucketerContext;
    private final Map<BucketID, LakeSoulBucket<IN, BucketID>> activeBuckets;
    private long maxPartCounter;
    private final OutputFileConfig outputFileConfig;
    private String rowKey;
    @Nullable
    private LakeSoulBucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener;
    @Nullable
    private FileLifeCycleListener<BucketID> fileLifeCycleListener;
    private final BucketStateSerializer<BucketID> bucketStateSerializer;

    public LakeSoulBuckets(Path basePath, BucketAssigner<IN, BucketID> bucketAssigner, LakeSoulBucketFactory<IN, BucketID> bucketFactory, BucketWriter<IN, BucketID> bucketWriter, LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy, int subtaskIndex, OutputFileConfig outputFileConfig) {
        this.basePath = (Path) Preconditions.checkNotNull(basePath);
        this.bucketAssigner = (BucketAssigner)Preconditions.checkNotNull(bucketAssigner);
        this.bucketFactory = (LakeSoulBucketFactory<IN, BucketID>) Preconditions.checkNotNull(bucketFactory);
        this.bucketWriter = (BucketWriter)Preconditions.checkNotNull(bucketWriter);
        this.rollingPolicy = (LakeSoulRollingPolicyImpl)Preconditions.checkNotNull(rollingPolicy);
        this.subtaskIndex = subtaskIndex;
        this.outputFileConfig = (OutputFileConfig)Preconditions.checkNotNull(outputFileConfig);
        this.activeBuckets = new HashMap();
        this.bucketerContext = new BucketerContext();
        this.bucketStateSerializer = new BucketStateSerializer(bucketWriter.getProperties().getInProgressFileRecoverableSerializer(), bucketWriter.getProperties().getPendingFileRecoverableSerializer(), bucketAssigner.getSerializer());
        this.maxPartCounter = 0L;

    }

    public void setBucketLifeCycleListener(LakeSoulBucketLifeCycleListener<IN, BucketID> bucketLifeCycleListener) {
        this.bucketLifeCycleListener = (LakeSoulBucketLifeCycleListener)Preconditions.checkNotNull(bucketLifeCycleListener);
    }

    public void setFileLifeCycleListener(FileLifeCycleListener<BucketID> fileLifeCycleListener) {
        this.fileLifeCycleListener = (FileLifeCycleListener)Preconditions.checkNotNull(fileLifeCycleListener);
    }

    public void initializeState(ListState<byte[]> bucketStates, ListState<Long> partCounterState) throws Exception {
        this.initializePartCounter(partCounterState);
        LOG.info("Subtask {} initializing its state (max part counter={}).", this.subtaskIndex, this.maxPartCounter);
        this.initializeActiveBuckets(bucketStates);
    }

    private void initializePartCounter(ListState<Long> partCounterState) throws Exception {
        long maxCounter = 0L;

        long partCounter;
        for(Iterator var4 = ((Iterable)partCounterState.get()).iterator(); var4.hasNext(); maxCounter = Math.max(partCounter, maxCounter)) {
            partCounter = (Long)var4.next();
        }

        this.maxPartCounter = maxCounter;
    }

    private void initializeActiveBuckets(ListState<byte[]> bucketStates) throws Exception {
        Iterator var2 = ((Iterable)bucketStates.get()).iterator();

        while(var2.hasNext()) {
            byte[] serializedRecoveredState = (byte[])var2.next();
            BucketState<BucketID> recoveredState = (BucketState) SimpleVersionedSerialization.readVersionAndDeSerialize(this.bucketStateSerializer, serializedRecoveredState);
            this.handleRestoredBucketState(recoveredState);
        }

    }

    private void handleRestoredBucketState(BucketState<BucketID> recoveredState) throws Exception {
        BucketID bucketId = recoveredState.getBucketId();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Subtask {} restoring: {}", this.subtaskIndex, recoveredState);
        }

        LakeSoulBucket<IN, BucketID> restoredLakeSoulBucket = this.bucketFactory.restoreBucket(this.subtaskIndex, this.maxPartCounter, this.bucketWriter, this.rollingPolicy, recoveredState, this.fileLifeCycleListener, this.outputFileConfig,this.rowKey);

        this.updateActiveBucketId(bucketId, restoredLakeSoulBucket);
    }

    private void updateActiveBucketId(BucketID bucketId, LakeSoulBucket<IN, BucketID> restoredLakeSoulBucket) throws IOException {
        if (!restoredLakeSoulBucket.isActive()) {
            this.notifyBucketInactive(restoredLakeSoulBucket);
        } else {
            LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)this.activeBuckets.get(bucketId);
            if (lakeSoulBucket != null) {
                lakeSoulBucket.merge(restoredLakeSoulBucket);
            } else {
                this.activeBuckets.put(bucketId, restoredLakeSoulBucket);
            }

        }
    }

    public void commitUpToCheckpoint(long checkpointId) throws IOException {
        Iterator<Map.Entry<BucketID, LakeSoulBucket<IN, BucketID>>> activeBucketIt = this.activeBuckets.entrySet().iterator();
        LOG.info("Subtask {} received completion notification for checkpoint with id={}.", this.subtaskIndex, checkpointId);

        while(activeBucketIt.hasNext()) {
            LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)((Map.Entry)activeBucketIt.next()).getValue();
            lakeSoulBucket.onSuccessfulCompletionOfCheckpoint(checkpointId);
            if (!lakeSoulBucket.isActive()) {
                activeBucketIt.remove();
                this.notifyBucketInactive(lakeSoulBucket);
            }
        }

    }

    public void snapshotState(long checkpointId, ListState<byte[]> bucketStatesContainer, ListState<Long> partCounterStateContainer) throws Exception {
        Preconditions.checkState(this.bucketWriter != null && this.bucketStateSerializer != null, "sink has not been initialized");
        LOG.info("Subtask {} checkpointing for checkpoint with id={} (max part counter={}).", new Object[]{this.subtaskIndex, checkpointId, this.maxPartCounter});
        bucketStatesContainer.clear();
        partCounterStateContainer.clear();
        this.snapshotActiveBuckets(checkpointId, bucketStatesContainer);
        partCounterStateContainer.add(this.maxPartCounter);
    }

    private void snapshotActiveBuckets(long checkpointId, ListState<byte[]> bucketStatesContainer) throws Exception {
        Iterator var4 = this.activeBuckets.values().iterator();

        while(var4.hasNext()) {
            LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)var4.next();
            BucketState<BucketID> bucketState = lakeSoulBucket.onReceptionOfCheckpoint(checkpointId);
            byte[] serializedBucketState = SimpleVersionedSerialization.writeVersionAndSerialize(this.bucketStateSerializer, bucketState);
            bucketStatesContainer.add(serializedBucketState);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Subtask {} checkpointing: {}", this.subtaskIndex, bucketState);
            }
        }

    }

    @VisibleForTesting
    public LakeSoulBucket<IN, BucketID> onElement(IN value, SinkFunction.Context context) throws Exception {
        return this.onElement(value, context.currentProcessingTime(), context.timestamp(), context.currentWatermark());
    }

    public LakeSoulBucket<IN, BucketID> onElement(IN value, long currentProcessingTime, @Nullable Long elementTimestamp, long currentWatermark) throws Exception {
        this.bucketerContext.update(elementTimestamp, currentWatermark, currentProcessingTime);
        BucketID bucketId = this.bucketAssigner.getBucketId(value, this.bucketerContext);
        LakeSoulBucket<IN, BucketID> lakeSoulBucket = this.getOrCreateBucketForBucketId(bucketId);
        lakeSoulBucket.write(value, currentProcessingTime);
        this.maxPartCounter = Math.max(this.maxPartCounter, lakeSoulBucket.getPartCounter());
        return lakeSoulBucket;
    }

    private LakeSoulBucket<IN, BucketID> getOrCreateBucketForBucketId(BucketID bucketId) throws IOException {
        LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)this.activeBuckets.get(bucketId);
        if (lakeSoulBucket == null) {
            Path bucketPath = this.assembleBucketPath(bucketId);
            lakeSoulBucket = this.bucketFactory.getNewBucket(this.subtaskIndex, bucketId, bucketPath, this.maxPartCounter, this.bucketWriter, this.rollingPolicy, this.fileLifeCycleListener, this.outputFileConfig , this.rowKey);
            this.activeBuckets.put(bucketId, lakeSoulBucket);
            this.notifyBucketCreate(lakeSoulBucket);
        }

        return lakeSoulBucket;
    }

    public void onProcessingTime(long timestamp) throws Exception {
        Iterator var3 = this.activeBuckets.values().iterator();

        while(var3.hasNext()) {
            LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)var3.next();
            lakeSoulBucket.onProcessingTime(timestamp);
        }

    }

    public void closePartFileForBucket(BucketID bucketID) throws Exception {
        LakeSoulBucket<IN, BucketID> lakeSoulBucket = (LakeSoulBucket)this.activeBuckets.get(bucketID);
        if (lakeSoulBucket != null) {
            lakeSoulBucket.closePartFile();
        }

    }

    public void close() {
        if (this.activeBuckets != null) {
            this.activeBuckets.values().forEach(LakeSoulBucket::disposePartFile);
        }

    }

    private Path assembleBucketPath(BucketID bucketId) {
        String child = bucketId.toString();
        return "".equals(child) ? this.basePath : new Path(this.basePath, child);
    }

    private void notifyBucketCreate(LakeSoulBucket<IN, BucketID> lakeSoulBucket) {
//        if (this.bucketLifeCycleListener != null) {
//            this.bucketLifeCycleListener.bucketCreated(lakeSoulBucket);
//        }

    }

    private void notifyBucketInactive(LakeSoulBucket<IN, BucketID> lakeSoulBucket) {
//        if (this.bucketLifeCycleListener != null) {
//            this.bucketLifeCycleListener.bucketInactive(lakeSoulBucket);
//        }

    }

    @VisibleForTesting
    public long getMaxPartCounter() {
        return this.maxPartCounter;
    }

    @VisibleForTesting
    Map<BucketID, LakeSoulBucket<IN, BucketID>> getActiveBuckets() {
        return this.activeBuckets;
    }

    private static final class BucketerContext implements org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context {
        @Nullable
        private Long elementTimestamp;
        private long currentWatermark;
        private long currentProcessingTime;

        private BucketerContext() {
            this.elementTimestamp = null;
            this.currentWatermark = -9223372036854775808L;
            this.currentProcessingTime = -9223372036854775808L;
        }

        void update(@Nullable Long elementTimestamp, long watermark, long processingTime) {
            this.elementTimestamp = elementTimestamp;
            this.currentWatermark = watermark;
            this.currentProcessingTime = processingTime;
        }

        @Override
        public long currentProcessingTime() {
            return this.currentProcessingTime;
        }

        @Override
        public long currentWatermark() {
            return this.currentWatermark;
        }

        @Override
        @Nullable
        public Long timestamp() {
            return this.elementTimestamp;
        }
    }
    public void setRowKey(String rowKey){
        this.rowKey=rowKey;
    }
}
