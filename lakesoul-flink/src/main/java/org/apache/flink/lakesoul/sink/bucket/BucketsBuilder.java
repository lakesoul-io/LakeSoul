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

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.committer.LakeSoulSinkCommitter;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.writer.AbstractLakeSoulMultiTableSinkWriter;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;

import java.io.IOException;
import java.io.Serializable;

/**
 * The base abstract class for the {@link BulkFormatBuilder}.
 */
public abstract class BucketsBuilder<IN, T extends BucketsBuilder<IN, T>>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

    @SuppressWarnings("unchecked")
    protected T self() {
        return (T) this;
    }

    public abstract AbstractLakeSoulMultiTableSinkWriter<IN> createWriter(final Sink.InitContext context, int subTaskId) throws IOException;

    public abstract LakeSoulSinkCommitter createCommitter() throws IOException;

    public abstract SimpleVersionedSerializer<LakeSoulWriterBucketState> getWriterStateSerializer()
            throws IOException;

    public abstract SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> getCommittableSerializer()
            throws IOException;
}
