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

package org.apache.flink.lakesoul;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.stream.StreamingFileWriter;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.filesystem.TableMetaStoreFactory;
import java.io.Serializable;
import java.util.List;

/** Helper for creating streaming file sink. */
@Internal
public class LakesoulSink {
    private LakesoulSink() {}

    public static <T> DataStream<DataInfo> writer(
            long bucketCheckInterval,
            DataStream<T> inputStream,
            LakesoulFileSink.BucketsBuilder<T, String, ? extends LakesoulFileSink.BucketsBuilder<T,?,?>>
                    bucketsBuilder,
            OutputFileConfig outputFile,
            int parallelism,
            List<String> partitionKeys,
            Configuration conf) {
        LakesoulFileWriter<T> fileWriter =
                new LakesoulFileWriter<T>(bucketCheckInterval, bucketsBuilder, partitionKeys, conf,outputFile);
        return inputStream
                .transform(LakesoulFileWriter.class.getSimpleName(),
                        TypeInformation.of(DataInfo.class),
                        fileWriter)
                .setParallelism(parallelism);
    }



    /**
     * Create a sink from file writer. Decide whether to add the node to commit partitions according
     * to options.
     */
    public static DataStreamSink<?> sink(
            DataStream<DataInfo> writer,
            Path locationPath,
            ObjectIdentifier identifier,
            List<String> partitionKeys,
            FileSystemFactory fsFactory,
            Configuration options) {
        DataStream<?> stream = null;
        //if (partitionKeys.size() > 0 && options.contains(SINK_PARTITION_COMMIT_POLICY_KIND)) {
        if (partitionKeys.size() > 0) {
                DataInfoCommitter committer =
                    new DataInfoCommitter(
                            locationPath, identifier, partitionKeys, fsFactory, options);
            stream =
                    writer.transform(
                            DataInfoCommitter.class.getSimpleName(), Types.VOID, committer)
                            .setParallelism(1)
                            .setMaxParallelism(1);
        }

        return stream.addSink(new DiscardingSink<>())
                .name("end")
                .setParallelism(1);
    }
}
