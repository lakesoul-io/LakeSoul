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

package org.apache.flink.lakesoul.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;

import java.util.List;

import org.apache.flink.lakesoul.metaData.DataInfo;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.BUCKET_PARALLELISM;

/**
 * Helper for creating streaming file sink.
 */

public class LakeSoulSink {

  public static <T> DataStream<DataInfo> writer(
      long bucketCheckInterval, DataStream<T> inputStream,
      LakeSoulBucketsBuilder<T, String, ? extends LakeSoulBucketsBuilder<T, ?, ?>> bucketsBuilder,
      OutputFileConfig outputFile,
      List<String> partitionKeys,
      Configuration conf) {
    LakSoulFileWriter<T> fileWriter =
        new LakSoulFileWriter<>(bucketCheckInterval, bucketsBuilder, partitionKeys, conf, outputFile);
    int bucketParallelism = conf.getInteger(BUCKET_PARALLELISM);

    return inputStream
        .transform(LakSoulFileWriter.class.getSimpleName(),
            TypeInformation.of(DataInfo.class),
            fileWriter).name("DataWrite")
        .setParallelism(bucketParallelism);
  }

  /**
   * Create a sink from file writer. Decide whether to add the node to commit partitions according
   * to options.
   */
  public static DataStreamSink<?> sink(
      DataStream<DataInfo> writer, Path locationPath,
      List<String> partitionKeys, Configuration options) {
    DataStream<?> stream = null;
    if (partitionKeys.size() > 0) {
      MetaDataCommit committer = new MetaDataCommit(locationPath, options);
      stream = writer.transform(
              MetaDataCommit.class.getSimpleName(), Types.VOID, committer)
          .setParallelism(1).name("DataCommit")
          .setMaxParallelism(1);
    }
    assert stream != null;
    return stream.addSink(new DiscardingSink<>())
        .name("end")
        .setParallelism(1);
  }

}
