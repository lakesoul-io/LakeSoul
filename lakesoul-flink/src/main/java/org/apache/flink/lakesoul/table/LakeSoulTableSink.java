/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.table;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.HashPartitioner;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTablesSink;
import org.apache.flink.lakesoul.sink.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class LakeSoulTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
  private final String tableName;
  private final EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
  private boolean overwrite;
  private final DataType dataType;
  private final ResolvedSchema schema;
  private final Configuration flinkConf;
  private final List<String> primaryKeyList;
  private final List<String> partitionKeyList;

  private static LakeSoulTableSink createLakesoulTableSink(LakeSoulTableSink lts) {
    return new LakeSoulTableSink(lts);
  }

  public LakeSoulTableSink(
          String tableName, DataType dataType,
          List<String> primaryKeyList, List<String> partitionKeyList,
          ReadableConfig flinkConf,
          EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
          ResolvedSchema schema
  ) {
    this.tableName = tableName;
    this.primaryKeyList = primaryKeyList;
    this.bulkWriterFormat = bulkWriterFormat;
    this.schema = schema;
    this.partitionKeyList = partitionKeyList;
    this.flinkConf = (Configuration) flinkConf;
    this.dataType = dataType;
  }

  private LakeSoulTableSink(LakeSoulTableSink tableSink) {
    this.tableName = tableSink.tableName;
    this.bulkWriterFormat = tableSink.bulkWriterFormat;
    this.overwrite = tableSink.overwrite;
    this.dataType = tableSink.dataType;
    this.schema = tableSink.schema;
    this.flinkConf = tableSink.flinkConf;
    this.primaryKeyList = tableSink.primaryKeyList;
    this.partitionKeyList = tableSink.partitionKeyList;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) (dataStream) -> {
      try {
        return createStreamingSink(dataStream, context);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : changelogMode.getContainedKinds()) {
      builder.addContainedKind(kind);
    }
    return builder.build();
  }

  /**
   * DataStream sink fileSystem and upload metadata
   */
  private DataStreamSink<?> createStreamingSink(DataStream<RowData> dataStream,
                                                Context sinkContext) throws IOException {
    Path path = FlinkUtil.makeQualifiedPath(new Path(flinkConf.getString(CATALOG_PATH)));
    int bucketParallelism = flinkConf.getInteger(BUCKET_PARALLELISM);
    //rowData key tools
    RowType rowType = (RowType) schema.toSourceRowDataType().notNull().getLogicalType();
    LakeSoulKeyGen keyGen = new LakeSoulKeyGen(
            rowType,
            primaryKeyList.toArray(new String[0])
            );
    //bucket file name config
    OutputFileConfig fileNameConfig = OutputFileConfig.builder()
                                                      .withPartSuffix(".parquet")
                                                      .build();
    //file rolling rule
    LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(
        flinkConf.getLong(FILE_ROLLING_SIZE), flinkConf.getLong(FILE_ROLLING_TIME));
    //redistribution by partitionKey
    dataStream = dataStream.partitionCustom(new HashPartitioner(), keyGen::getRePartitionHash);
    //rowData sink fileSystem Task
    LakeSoulMultiTablesSink<RowData> sink = LakeSoulMultiTablesSink.forOneTableBulkFormat(
               path,
               new TableSchemaIdentity(new TableId(io.debezium.relational.TableId.parse(tableName)),
                                       rowType,
                                       path.toString(),
                                       primaryKeyList,
                                       partitionKeyList),
               flinkConf)
       .withBucketCheckInterval(flinkConf.getLong(BUCKET_CHECK_INTERVAL))
       .withRollingPolicy(rollingPolicy)
       .withOutputFileConfig(fileNameConfig)
       .build();
    return dataStream.sinkTo(sink).setParallelism(bucketParallelism);
  }

  @Override
  public DynamicTableSink copy() {
    return createLakesoulTableSink(this);
  }

  @Override
  public String asSummaryString() {
    return "lakeSoul table sink";
  }

  @Override
  public void applyOverwrite(boolean newOverwrite) {
    this.overwrite = newOverwrite;
  }

  @Override
  public void applyStaticPartition(Map<String, String> map) {
  }

  @Override
  public boolean requiresPartitionGrouping(boolean supportsGrouping) {
    return false;
  }

}
