// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulMultiTablesSink<IN, OUT> implements
        Sink<IN, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState,
                LakeSoulMultiTableSinkGlobalCommittable> {

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
    public SinkWriter<IN, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState> createWriter(
            InitContext context, List<LakeSoulWriterBucketState> states) throws IOException {
        int subTaskId = context.getSubtaskId();
        AbstractLakeSoulMultiTableSinkWriter<IN, OUT> writer = bucketsBuilder.createWriter(context, subTaskId);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulWriterBucketState>> getWriterStateSerializer() {
        try {
            return Optional.of(bucketsBuilder.getWriterStateSerializer());
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
    public Optional<Committer<LakeSoulMultiTableSinkCommittable>> createCommitter() throws IOException {
        return Optional.of(new Committer<LakeSoulMultiTableSinkCommittable>() {
            @Override
            public List<LakeSoulMultiTableSinkCommittable> commit(List<LakeSoulMultiTableSinkCommittable> committables)
                    throws IOException, InterruptedException {
                return Collections.emptyList();
            }

            @Override
            public void close() throws Exception {
            }
        });
    }

    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable>> getCommittableSerializer() {
        try {
            return Optional.of(bucketsBuilder.getCommittableSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // LakeSoulMultiTablesSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public Optional<GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable>> createGlobalCommitter()
            throws IOException {
        return Optional.ofNullable(bucketsBuilder.createGlobalCommitter());
    }

    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable>> getGlobalCommittableSerializer() {
        try {
            return Optional.of(bucketsBuilder.getGlobalCommittableSerializer());
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // LakeSoulMultiTablesSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create global committable serializer.", e);
        }
    }

    @Override
    public Collection<String> getCompatibleStateNames() {
        // StreamingFileSink
        return Collections.singleton("lakesoul-cdc-multitable-bucket-states");
    }
}
