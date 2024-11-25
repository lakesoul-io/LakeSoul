// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.bucket.BucketsBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultMultiTablesArrowFormatBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultMultiTablesBulkFormatBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultOneTableBulkFormatBuilder;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkGlobalCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.StandardSinkTopologies;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulMultiTablesSink<IN, OUT> implements
        StatefulSink<IN, LakeSoulWriterBucketState>,
        TwoPhaseCommittingSink<IN, LakeSoulMultiTableSinkCommittable>,
        WithPostCommitTopology<IN, LakeSoulMultiTableSinkCommittable> {

    private final BucketsBuilder<IN, OUT, ? extends BucketsBuilder<IN, OUT, ?>> bucketsBuilder;

    public LakeSoulMultiTablesSink(BucketsBuilder<IN, OUT, ? extends BucketsBuilder<IN, OUT, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    public static DefaultOneTableBulkFormatBuilder forOneTableBulkFormat(final Path basePath,
                                                                         TableSchemaIdentity identity,
                                                                         Configuration conf) {
        return new DefaultOneTableBulkFormatBuilder(identity, basePath, conf);
    }

    public static DefaultMultiTablesBulkFormatBuilder forMultiTablesBulkFormat(Configuration conf) {
        return new DefaultMultiTablesBulkFormatBuilder(new Path(conf.getString(LakeSoulSinkOptions.WAREHOUSE_PATH)),
                conf);
    }

    public static DefaultMultiTablesArrowFormatBuilder forMultiTablesArrowFormat(Configuration conf) {
        return new DefaultMultiTablesArrowFormatBuilder(new Path(conf.getString(LakeSoulSinkOptions.WAREHOUSE_PATH)),
                conf);
    }

    @Override
    public AbstractLakeSoulMultiTableSinkWriter<IN, OUT> createWriter(InitContext context) throws IOException {
        int subTaskId = context.getSubtaskId();
        AbstractLakeSoulMultiTableSinkWriter<IN, OUT> writer = bucketsBuilder.createWriter(context, subTaskId);
        return writer;
    }

    @Override
    public StatefulSinkWriter<IN, LakeSoulWriterBucketState> restoreWriter(InitContext context, Collection<LakeSoulWriterBucketState> recoveredState) throws IOException {
        int subTaskId = context.getSubtaskId();
        AbstractLakeSoulMultiTableSinkWriter<IN, OUT> writer = bucketsBuilder.createWriter(context, subTaskId);
        writer.initializeState(new ArrayList<>(recoveredState));
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulWriterBucketState> getWriterStateSerializer() {
        try {
            return bucketsBuilder.getWriterStateSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // LakeSoulMultiTablesSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    // committer must not be null since flink requires it to enable
    // StatefulGlobalTwoPhaseCommittingSinkAdapter
    @Override
    public Committer<LakeSoulMultiTableSinkCommittable> createCommitter() throws IOException {
        return new Committer<LakeSoulMultiTableSinkCommittable>() {
            @Override
            public void close() throws Exception {
            }
            @Override
            public void commit(Collection<CommitRequest<LakeSoulMultiTableSinkCommittable>> committables) throws IOException, InterruptedException {
            }
        };
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> getCommittableSerializer() {
        try {
            return bucketsBuilder.getCommittableSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // LakeSoulMultiTablesSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    public BucketsBuilder getBucketsBuilder(){
        return this.bucketsBuilder;
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<LakeSoulMultiTableSinkCommittable>> committables) {
        StandardSinkTopologies.addGlobalCommitter(
                committables,
                GlobalCommitterAdapter::new,
                this::getCommittableSerializer);
    }

    public class GlobalCommitterAdapter implements Committer<LakeSoulMultiTableSinkCommittable> {
        final GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable> globalCommitter;
        final SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable> globalCommittableSerializer;

        GlobalCommitterAdapter() {
            try {
                globalCommitter = LakeSoulMultiTablesSink.this.bucketsBuilder.createGlobalCommitter();
                globalCommittableSerializer = LakeSoulMultiTablesSink.this.bucketsBuilder.getGlobalCommittableSerializer();
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot create global committer", e);
            }
        }

        @Override
        public void close() throws Exception {
            globalCommitter.close();
        }

        @Override
        public void commit(Collection<CommitRequest<LakeSoulMultiTableSinkCommittable>> committables)
                throws IOException, InterruptedException {
            if (committables.isEmpty()) {
                return;
            }

            List<LakeSoulMultiTableSinkCommittable> rawCommittables =
                    committables.stream()
                            .map(CommitRequest::getCommittable)
                            .collect(Collectors.toList());
            List<LakeSoulMultiTableSinkGlobalCommittable> globalCommittables =
                    Collections.singletonList(globalCommitter.combine(rawCommittables));
            List<LakeSoulMultiTableSinkGlobalCommittable> failures = globalCommitter.commit(globalCommittables);
            // Only committables are retriable so the complete batch of committables is retried
            // because we cannot trace back the committable to which global committable it belongs.
            // This might lead to committing the same global committable twice, but we assume that
            // the GlobalCommitter commit call is idempotent.
            if (!failures.isEmpty()) {
                committables.forEach(CommitRequest::retryLater);
            }
        }

        public GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable> getGlobalCommitter() {
            return globalCommitter;
        }

        public SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable> getGlobalCommittableSerializer() {
            return globalCommittableSerializer;
        }
    }
}
