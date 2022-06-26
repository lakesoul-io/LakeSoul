package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class LakeSoulBucket<IN, BucketID>{
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.api.functions.sink.filesystem.Bucket.class);
    private final BucketID bucketId;
    private final Path bucketPath;
    private final int subtaskIndex;
    private final BucketWriter<IN, BucketID> bucketWriter;
    private final RollingPolicy<IN, BucketID> rollingPolicy;
    private final NavigableMap<Long, InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverablesPerCheckpoint;
    private final NavigableMap<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablesPerCheckpoint;
    private final OutputFileConfig outputFileConfig;
    private PriorityQueue<RowData> sortQueue;
    private AtomicLong bucketCount;
    private  final LakeSoulKeyGen keygen;
    @Nullable
    private final FileLifeCycleListener<BucketID> fileListener;
    private long partCounter;
    @Nullable
    private InProgressFileWriter<IN, BucketID> inProgressPart;
    private List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverablesForCurrentCheckpoint;

    private LakeSoulBucket(int subtaskIndex, BucketID bucketId, Path bucketPath, long initialPartCounter, BucketWriter<IN, BucketID> bucketWriter, RollingPolicy<IN, BucketID> rollingPolicy, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig){
        this.subtaskIndex = subtaskIndex;
        this.bucketId = Preconditions.checkNotNull(bucketId);
        this.bucketPath = (Path)Preconditions.checkNotNull(bucketPath);
        this.partCounter = initialPartCounter;
        this.bucketWriter = (BucketWriter)Preconditions.checkNotNull(bucketWriter);
        this.rollingPolicy = (RollingPolicy)Preconditions.checkNotNull(rollingPolicy);
        this.fileListener = fileListener;
        this.pendingFileRecoverablesForCurrentCheckpoint = new ArrayList();
        this.pendingFileRecoverablesPerCheckpoint = new TreeMap();
        this.inProgressFileRecoverablesPerCheckpoint = new TreeMap();
        this.outputFileConfig = (OutputFileConfig)Preconditions.checkNotNull(outputFileConfig);
        LakeSoulRollingPolicyImpl lakesoulRollingPolicy = (LakeSoulRollingPolicyImpl) rollingPolicy;
        this.keygen = lakesoulRollingPolicy.getKeyGen();
            sortQueue = new PriorityQueue<>((v1, v2) -> {
                try {
                    return keygen.getRecordKey(v1).compareTo(keygen.getRecordKey(v2));
                } catch (Exception e) {
                    e.printStackTrace();
                    return 0;
                }
            });
        bucketCount=new AtomicLong(0L);
    }

    private LakeSoulBucket(int subtaskIndex, long initialPartCounter, BucketWriter<IN, BucketID> partFileFactory, RollingPolicy<IN, BucketID> rollingPolicy, BucketState<BucketID> bucketState, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig) throws IOException {
        this(subtaskIndex, bucketState.getBucketId(), bucketState.getBucketPath(), initialPartCounter, partFileFactory, rollingPolicy, fileListener, outputFileConfig);
        this.restoreInProgressFile(bucketState);
        this.commitRecoveredPendingFiles(bucketState);
    }

    private void restoreInProgressFile(BucketState<BucketID> state) throws IOException {
        if (state.hasInProgressFileRecoverable()) {
            InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = state.getInProgressFileRecoverable();
            if (this.bucketWriter.getProperties().supportsResume()) {
                this.inProgressPart = this.bucketWriter.resumeInProgressFileFrom(this.bucketId, inProgressFileRecoverable, state.getInProgressFileCreationTime());
            } else {
                this.bucketWriter.recoverPendingFile(inProgressFileRecoverable).commitAfterRecovery();
            }

        }
    }

    private void commitRecoveredPendingFiles(BucketState<BucketID> state) throws IOException {
        Iterator var2 = state.getPendingFileRecoverablesPerCheckpoint().values().iterator();

        while(var2.hasNext()) {
            List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables = (List)var2.next();
            Iterator var4 = pendingFileRecoverables.iterator();

            while(var4.hasNext()) {
                InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = (InProgressFileWriter.PendingFileRecoverable)var4.next();
                this.bucketWriter.recoverPendingFile(pendingFileRecoverable).commitAfterRecovery();
            }
        }

    }

    public BucketID getBucketId() {
        return this.bucketId;
    }

    public Path getBucketPath() {
        return this.bucketPath;
    }

    public long getPartCounter() {
        return this.partCounter;
    }

    boolean isActive() {
        return this.inProgressPart != null || !this.pendingFileRecoverablesForCurrentCheckpoint.isEmpty() || !this.pendingFileRecoverablesPerCheckpoint.isEmpty();
    }

    void merge(LakeSoulBucket<IN, BucketID> lakeSoulBucket) throws IOException {
        Preconditions.checkNotNull(lakeSoulBucket);
        Preconditions.checkState(Objects.equals(lakeSoulBucket.bucketPath, this.bucketPath));
        Preconditions.checkState(lakeSoulBucket.pendingFileRecoverablesForCurrentCheckpoint.isEmpty());
        Preconditions.checkState(lakeSoulBucket.pendingFileRecoverablesPerCheckpoint.isEmpty());
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = lakeSoulBucket.closePartFile();
        if (pendingFileRecoverable != null) {
            this.pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Subtask {} merging buckets for lakeSoulBucket id={}", this.subtaskIndex, this.bucketId);
        }

    }

    void write(IN element, long currentTime) throws IOException {
        if (this.inProgressPart == null ){
            this.inProgressPart = this.newPartFile(currentTime);
        } else if (
                this.rollingPolicy.shouldRollOnProcessingTime(this.inProgressPart, currentTime) ||
                        checkRollingPolicy()
        ){
            sortWrite(currentTime);
            this.inProgressPart = this.rollPartFile(currentTime);
            bucketCount=new AtomicLong(0L);
        }
        sortQueue.add((RowData) element);

    }

    private boolean checkRollingPolicy(){
        if(rollingPolicy instanceof LakeSoulRollingPolicyImpl){
            LakeSoulRollingPolicyImpl<?, ?> LakeSoulRollingPolicy = (LakeSoulRollingPolicyImpl<?, ?>) this.rollingPolicy;
            return LakeSoulRollingPolicy.shouldRollOnMaxSize(this.bucketCount.getAndIncrement());
        }else {
            return false;
        }
    }

    void sortWrite(long currentTime)throws IOException {
        while(!sortQueue.isEmpty()){
            RowData poll = sortQueue.poll();
            this.inProgressPart.write((IN) poll,currentTime);
        };
    }

    private InProgressFileWriter<IN, BucketID> rollPartFile(long currentTime) throws IOException {
        this.closePartFile(currentTime);
        return newPartFile(currentTime);
    }

    private InProgressFileWriter<IN, BucketID> newPartFile(long currentTime) throws IOException {
        Path partFilePath = this.assembleNewPartPath();
        if (this.fileListener != null) {
            this.fileListener.onPartFileOpened(this.bucketId, partFilePath);
        }
        return this.bucketWriter.openNewInProgressFile(this.bucketId, partFilePath, currentTime);
    }

    private Path assembleNewPartPath() {
        long currentPartCounter = (long)(this.partCounter++);
        String uuid =  UUID.randomUUID().toString();
        String subTask = String.format("%05d", this.subtaskIndex);
        String count = String.format("%03d", currentPartCounter);

        return new Path(this.bucketPath, this.outputFileConfig.getPartPrefix() + '-' + subTask + '-' + uuid+ '_' + this.subtaskIndex +'.'+'c'+count + this.outputFileConfig.getPartSuffix());
    }

    InProgressFileWriter.PendingFileRecoverable closePartFile(long currentTime) throws IOException {
        if(!sortQueue.isEmpty()&&this.inProgressPart!=null){
            sortWrite(currentTime);
        }
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = null;
        if (this.inProgressPart != null) {
            pendingFileRecoverable = this.inProgressPart.closeForCommit();
            this.pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
            this.inProgressPart = null;
        }

        return pendingFileRecoverable;
    }

    InProgressFileWriter.PendingFileRecoverable closePartFile() throws IOException {
        if(!sortQueue.isEmpty()&&this.inProgressPart!=null){
            sortWrite(inProgressPart.getLastUpdateTime());
        }
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = null;
        if (this.inProgressPart != null) {
            pendingFileRecoverable = this.inProgressPart.closeForCommit();
            this.pendingFileRecoverablesForCurrentCheckpoint.add(pendingFileRecoverable);
            this.inProgressPart = null;
        }

        return pendingFileRecoverable;
    }

    void disposePartFile() {
        if (this.inProgressPart != null) {
            this.inProgressPart.dispose();
        }

    }

    BucketState<BucketID> onReceptionOfCheckpoint(long checkpointId) throws IOException {
        this.prepareBucketForCheckpointing(checkpointId);
        InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = null;
        long inProgressFileCreationTime = 9223372036854775807L;
        if (this.inProgressPart != null) {
            inProgressFileRecoverable = this.inProgressPart.persist();
            inProgressFileCreationTime = this.inProgressPart.getCreationTime();
            this.inProgressFileRecoverablesPerCheckpoint.put(checkpointId, inProgressFileRecoverable);
        }

        return new BucketState(this.bucketId, this.bucketPath, inProgressFileCreationTime, inProgressFileRecoverable, this.pendingFileRecoverablesPerCheckpoint);
    }

    private void prepareBucketForCheckpointing(long checkpointId) throws IOException {
        if (this.inProgressPart != null && this.rollingPolicy.shouldRollOnCheckpoint(this.inProgressPart)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Subtask {} closing in-progress part file for bucket id={} on checkpoint.", this.subtaskIndex, this.bucketId);
            }

            this.closePartFile();
        }

        if (!this.pendingFileRecoverablesForCurrentCheckpoint.isEmpty()) {
            this.pendingFileRecoverablesPerCheckpoint.put(checkpointId, this.pendingFileRecoverablesForCurrentCheckpoint);
            this.pendingFileRecoverablesForCurrentCheckpoint = new ArrayList();
        }

    }

    void onSuccessfulCompletionOfCheckpoint(long checkpointId) throws IOException {
        Preconditions.checkNotNull(this.bucketWriter);
        Iterator it = this.pendingFileRecoverablesPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

        while(it.hasNext()) {
            Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> entry = (Map.Entry)it.next();
            Iterator var5 = ((List)entry.getValue()).iterator();

            while(var5.hasNext()) {
                InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = (InProgressFileWriter.PendingFileRecoverable)var5.next();
                this.bucketWriter.recoverPendingFile(pendingFileRecoverable).commit();
            }

            it.remove();
        }

        this.cleanupInProgressFileRecoverables(checkpointId);
    }

    private void cleanupInProgressFileRecoverables(long checkpointId) throws IOException {
        for(Iterator it = this.inProgressFileRecoverablesPerCheckpoint.headMap(checkpointId, false).entrySet().iterator(); it.hasNext(); it.remove()) {
            InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = (InProgressFileWriter.InProgressFileRecoverable)((Map.Entry)it.next()).getValue();
            boolean successfullyDeleted = this.bucketWriter.cleanupInProgressFileRecoverable(inProgressFileRecoverable);
            if (LOG.isDebugEnabled() && successfullyDeleted) {
                LOG.debug("Subtask {} successfully deleted incomplete part for bucket id={}.", this.subtaskIndex, this.bucketId);
            }
        }

    }

    void onProcessingTime(long timestamp) throws IOException {
        if (this.inProgressPart != null && this.rollingPolicy.shouldRollOnProcessingTime(this.inProgressPart, timestamp)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Subtask {} closing in-progress part file for bucket id={} due to processing time rolling policy (in-progress file created @ {}, last updated @ {} and current time is {}).", new Object[]{this.subtaskIndex, this.bucketId, this.inProgressPart.getCreationTime(), this.inProgressPart.getLastUpdateTime(), timestamp});
            }

            this.closePartFile();
        }

    }

    @VisibleForTesting
    Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> getPendingFileRecoverablesPerCheckpoint() {
        return this.pendingFileRecoverablesPerCheckpoint;
    }

    @Nullable
    @VisibleForTesting
    InProgressFileWriter<IN, BucketID> getInProgressPart() {
        return this.inProgressPart;
    }

    @VisibleForTesting
    List<InProgressFileWriter.PendingFileRecoverable> getPendingFileRecoverablesForCurrentCheckpoint() {
        return this.pendingFileRecoverablesForCurrentCheckpoint;
    }

    static <IN, BucketID> LakeSoulBucket<IN, BucketID> getNew(int subtaskIndex, BucketID bucketId, Path bucketPath, long initialPartCounter, BucketWriter<IN, BucketID> bucketWriter, LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig) {
        return new LakeSoulBucket(subtaskIndex, bucketId, bucketPath, initialPartCounter, bucketWriter, rollingPolicy, fileListener, outputFileConfig);
    }

    static <IN, BucketID> LakeSoulBucket<IN, BucketID> restore(int subtaskIndex, long initialPartCounter, BucketWriter<IN, BucketID> bucketWriter, LakeSoulRollingPolicyImpl<IN, BucketID> rollingPolicy, BucketState<BucketID> bucketState, @Nullable FileLifeCycleListener<BucketID> fileListener, OutputFileConfig outputFileConfig) throws IOException {
        return new LakeSoulBucket(subtaskIndex, initialPartCounter, bucketWriter, rollingPolicy, bucketState, fileListener, outputFileConfig);
    }
}

