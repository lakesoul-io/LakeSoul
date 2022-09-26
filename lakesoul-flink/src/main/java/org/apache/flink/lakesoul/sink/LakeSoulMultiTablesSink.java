/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.bucket.BucketsBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultMultiTablesBulkFormatBuilder;
import org.apache.flink.lakesoul.sink.bucket.DefaultOneTableBulkFormatBuilder;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class LakeSoulMultiTablesSink<IN> implements Sink<IN, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState, Void> {

    private final BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder;

    public LakeSoulMultiTablesSink(BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public SinkWriter<IN, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState> createWriter(
            InitContext context, List<LakeSoulWriterBucketState> states) throws IOException {
        int subTaskId = context.getSubtaskId();
        AbstractLakeSoulMultiTableSinkWriter<IN> writer = bucketsBuilder.createWriter(context, subTaskId);
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

    @Override
    public Optional<Committer<LakeSoulMultiTableSinkCommittable>> createCommitter() throws IOException {
        return Optional.of(bucketsBuilder.createCommitter());
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
    public Optional<GlobalCommitter<LakeSoulMultiTableSinkCommittable, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Collection<String> getCompatibleStateNames() {
        // StreamingFileSink
        return Collections.singleton("lakesoul-cdc-multitable-bucket-states");
    }

    public static DefaultOneTableBulkFormatBuilder forOneTableBulkFormat(
            final Path basePath, TableSchemaIdentity identity, Configuration conf) {
        return new DefaultOneTableBulkFormatBuilder(
                identity, basePath, conf);
    }

    public static DefaultMultiTablesBulkFormatBuilder forMultiTablesBulkFormat(Configuration conf) {
        return new DefaultMultiTablesBulkFormatBuilder(
                new Path(conf.getString(LakeSoulSinkOptions.WAREHOUSE_PATH)), conf);
    }
}
