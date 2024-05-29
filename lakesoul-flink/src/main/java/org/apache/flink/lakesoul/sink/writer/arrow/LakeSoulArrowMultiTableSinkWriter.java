// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer.arrow;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.writer.DefaultLakeSoulWriterBucketFactory;
import org.apache.flink.lakesoul.sink.writer.TableSchemaWriterCreator;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKET;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.DYNAMIC_BUCKETING;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulArrowMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<LakeSoulArrowWrapper, LakeSoulArrowWrapper> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulArrowMultiTableSinkWriter.class);

    protected final LakeSoulArrowWriterBucketFactory arrowBucketFactory;
    private Map<TableSchemaIdentity, LakeSoulArrowWriterBucket> activeArrowBuckets;

    public LakeSoulArrowMultiTableSinkWriter(int subTaskId,
                                             SinkWriterMetricGroup metricGroup,
                                             LakeSoulArrowWriterBucketFactory bucketFactory,
                                             RollingPolicy<LakeSoulArrowWrapper, String> rollingPolicy,
                                             OutputFileConfig outputFileConfig,
                                             Sink.ProcessingTimeService processingTimeService,
                                             long bucketCheckInterval,
                                             Configuration conf) {
        super(subTaskId, metricGroup, new DefaultLakeSoulWriterBucketFactory(conf), rollingPolicy, outputFileConfig, processingTimeService,
                bucketCheckInterval, conf);
        arrowBucketFactory = bucketFactory;
        activeArrowBuckets = new HashMap<>();
    }

    @Override
    protected TableSchemaWriterCreator getOrCreateTableSchemaWriterCreator(TableSchemaIdentity identity) {
        throw new RuntimeException("getOrCreateTableSchemaWriterCreator not implemented");
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(LakeSoulArrowWrapper element) {
        throw new RuntimeException("extractTableSchemaAndRowData not implemented");
    }

    @Override
    public void write(LakeSoulArrowWrapper element, Context context) throws IOException {
        // setting the values in the bucketer context
        bucketerContext.update(
                context.timestamp(),
                context.currentWatermark(),
                processingTimeService.getCurrentProcessingTime());

        TableSchemaIdentity identity = element.generateTableSchemaIdentity();


        final LakeSoulArrowWriterBucket bucket = getOrCreateBucketForTableId(identity);
        if (conf.get(DYNAMIC_BUCKETING)) {
            bucket.write(element, processingTimeService.getCurrentProcessingTime(), Long.MAX_VALUE);

        } else {
            throw new RuntimeException("Static Bucketing Not Support");
        }
    }

    protected LakeSoulArrowWriterBucket getOrCreateBucketForTableId(TableSchemaIdentity identity) throws IOException {
        LakeSoulArrowWriterBucket bucket = activeArrowBuckets.get(identity);
        if (bucket == null) {
            final Path bucketPath = FlinkUtil.makeQualifiedPath(identity.tableLocation);
            BucketWriter<LakeSoulArrowWrapper, String> bucketWriter = new NativeArrowBucketWriter(identity.rowType, identity.primaryKeys, identity.partitionKeyList, conf);
            bucket = arrowBucketFactory.getNewBucket(
                    getSubTaskId(),
                    identity,
                    DYNAMIC_BUCKET,
                    bucketPath,
                    bucketWriter,
                    getRollingPolicy(),
                    getOutputFileConfig());
            activeArrowBuckets.put(identity, bucket);
            LOG.info("Create new bucket {}, {}",
                    identity, bucketPath);
        }
        return bucket;
    }

    public void initializeState(List<LakeSoulWriterBucketState> bucketStates) throws IOException {
        checkNotNull(bucketStates, "The retrieved state was null.");
        LOG.info("initializeState size {}", bucketStates.size());
        for (LakeSoulWriterBucketState state : bucketStates) {

            LOG.info("initializeState restoring state: {}", state);

            TableSchemaIdentity identity = state.getIdentity();
            BucketWriter<LakeSoulArrowWrapper, String> bucketWriter = new NativeArrowBucketWriter(identity.rowType, identity.primaryKeys, identity.partitionKeyList, conf);
            LakeSoulArrowWriterBucket restoredBucket =
                    arrowBucketFactory.restoreBucket(
                            getSubTaskId(),
                            state.getIdentity(),
                            bucketWriter,
                            getRollingPolicy(),
                            state,
                            getOutputFileConfig());

            updateActiveBucketId(identity, restoredBucket);
        }

        registerNextBucketInspectionTimer();
    }

    private void updateActiveBucketId(TableSchemaIdentity tableId, LakeSoulArrowWriterBucket restoredBucket)
            throws IOException {
        final LakeSoulArrowWriterBucket bucket = activeArrowBuckets.get(tableId);
        if (bucket != null) {
            bucket.merge(restoredBucket);
        } else {
            activeArrowBuckets.put(tableId, restoredBucket);
        }
    }

    @Override
    public List<LakeSoulWriterBucketState> snapshotState(long checkpointId) throws IOException {

        List<LakeSoulWriterBucketState> states = new ArrayList<>();
        for (LakeSoulArrowWriterBucket bucket : activeArrowBuckets.values()) {
            LakeSoulWriterBucketState state = bucket.snapshotState();
            states.add(state);
        }

        return states;
    }

    @Override
    public void close() {
        super.close();
        if (activeArrowBuckets != null) {
            activeArrowBuckets.values().forEach(LakeSoulArrowWriterBucket::disposePartFile);
        }
    }

    @Override
    public List<LakeSoulMultiTableSinkCommittable> prepareCommit(boolean flush) throws IOException {
        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        String dmlType = this.conf.getString(LakeSoulSinkOptions.DML_TYPE);
        String sourcePartitionInfo = this.conf.getString(LakeSoulSinkOptions.SOURCE_PARTITION_INFO);
        // Every time before we prepare commit, we first check and remove the inactive
        // buckets. Checking the activeness right before pre-committing avoid re-creating
        // the bucket every time if the bucket use OnCheckpointingRollingPolicy.
        Iterator<Map.Entry<TableSchemaIdentity, LakeSoulArrowWriterBucket>> activeBucketIt =
                activeArrowBuckets.entrySet().iterator();
        while (activeBucketIt.hasNext()) {
            Map.Entry<TableSchemaIdentity, LakeSoulArrowWriterBucket> entry = activeBucketIt.next();
            if (!entry.getValue().isActive()) {
                activeBucketIt.remove();
            } else {
                committables.addAll(entry.getValue().prepareCommit(flush, dmlType, sourcePartitionInfo));
            }
        }

        return committables;
    }

}
