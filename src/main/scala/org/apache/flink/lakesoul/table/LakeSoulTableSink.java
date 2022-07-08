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

import org.apache.flink.lakesoul.metaData.DataInfo;
import org.apache.flink.lakesoul.sink.LakeSoulSink;
import org.apache.flink.lakesoul.sink.LakeSoulFileSink;
import org.apache.flink.lakesoul.sink.fileSystem.FlinkBucketAssigner;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakesoul.sink.fileSystem.bulkFormat.ProjectionBulkFactory;
import org.apache.flink.lakesoul.sink.partition.BucketPartitioner;
import org.apache.flink.lakesoul.tools.ProjectionEncoder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.lakesoul.sink.partition.LakeSoulCdcPartitionComputer;
import org.apache.flink.lakesoul.tools.FlinkUtil;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ResolvedSchema;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;

import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.BUCKET_CHECK_INTERVAL;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.CATALOG_PATH;
import static org.apache.flink.lakesoul.tools.LakeSoulKeyGen.DEFAULT_PARTITION_PATH;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.FILE_ROLLING_SIZE;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.FILE_ROLLING_TIME;

public class LakeSoulTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
  private EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
  private EncodingFormat<SerializationSchema<RowData>> serializationFormat;
  private boolean overwrite;
  private Path path;
  private DataType DataType;
  private ResolvedSchema schema;
  private Configuration flinkConf;
  private LakeSoulKeyGen keyGen;
  private List<String> partitionKeyList;

  private static LakeSoulTableSink createLakesoulTableSink(LakeSoulTableSink lts) {
    return new LakeSoulTableSink(lts);
  }

  public LakeSoulTableSink(
      DataType DataType,
      List<String> partitionKeyList,
      ReadableConfig flinkConf,
      EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
      EncodingFormat<SerializationSchema<RowData>> serializationFormat,
      ResolvedSchema schema
  ) {
    this.serializationFormat = serializationFormat;
    this.bulkWriterFormat = bulkWriterFormat;
    this.schema = schema;
    this.partitionKeyList = partitionKeyList;
    this.flinkConf = (Configuration) flinkConf;
    this.DataType = DataType;
  }

  private LakeSoulTableSink(LakeSoulTableSink tableSink) {
    this.overwrite = tableSink.overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
    return (DataStreamSinkProvider) (dataStream) -> createStreamingSink(dataStream, sinkContext);
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : changelogMode.getContainedKinds()) {
      builder.addContainedKind(kind);
    }
    return builder.build();
  }

  private DataStreamSink<?> createStreamingSink(
      DataStream<RowData> dataStream,
      Context sinkContext) {
    keyGen = new LakeSoulKeyGen((RowType) schema.toSourceRowDataType().notNull().getLogicalType(), flinkConf);
    LakeSoulCdcPartitionComputer PartitionComputer = partitionCdcComputer();
    FlinkBucketAssigner assigner = new FlinkBucketAssigner(PartitionComputer);
    keyGen.partitionKey = partitionKeyList;
    Object writer = createWriter(sinkContext);
    boolean isEncoder = writer instanceof Encoder;
    long fileRollingSize = flinkConf.getLong(FILE_ROLLING_SIZE);
    long fileRollingTime = flinkConf.getLong(FILE_ROLLING_TIME);
    LakeSoulRollingPolicyImpl lakeSoulPolicy = new LakeSoulRollingPolicyImpl(!isEncoder,fileRollingSize,fileRollingTime);
    lakeSoulPolicy.setKeyGen(keyGen);
    this.path = new Path(flinkConf.getString(CATALOG_PATH));
    OutputFileConfig fileNameConfig = OutputFileConfig.builder().build();
    LakeSoulBucketsBuilder<RowData, String, ? extends LakeSoulBucketsBuilder<RowData, ?, ?>>
        bucketsBuilder = bucketsBuilderFactory(isEncoder, writer, PartitionComputer, assigner, fileNameConfig, lakeSoulPolicy);

    BucketPartitioner<String> partitioner = new BucketPartitioner<>();
    dataStream = dataStream.partitionCustom(partitioner, keyGen::getBucketPartitionKey);
    long bucketCheckInterval = flinkConf.getLong(BUCKET_CHECK_INTERVAL);
    DataStream<DataInfo> writerStream = LakeSoulSink.writer(
        bucketCheckInterval, dataStream, bucketsBuilder,
        fileNameConfig, partitionKeyList, flinkConf);
    return LakeSoulSink.sink(writerStream, this.path, partitionKeyList, flinkConf);
  }

  private LakeSoulBucketsBuilder<RowData, String, ? extends LakeSoulBucketsBuilder<RowData, ?, ?>> bucketsBuilderFactory(
      boolean isEncoder, Object writer, LakeSoulCdcPartitionComputer PartitionComputer, FlinkBucketAssigner BucketAssigner,
      OutputFileConfig fileNameConfig, LakeSoulRollingPolicyImpl lakeSoulPolicy
  ) {
    if (isEncoder) {
      //noinspection unchecked
      return
          LakeSoulFileSink.forRowFormat(
                  this.path,
                  new ProjectionEncoder((Encoder<RowData>) writer, PartitionComputer))
              .withBucketAssigner(BucketAssigner)
              .withOutputFileConfig(fileNameConfig)
              .withRollingPolicy(lakeSoulPolicy);
    } else {
      //noinspection unchecked
      return
          LakeSoulFileSink.forBulkFormat(
                  this.path,
                  new ProjectionBulkFactory(
                      (BulkWriter.Factory<RowData>) writer, PartitionComputer))
              .withBucketAssigner(BucketAssigner)
              .withOutputFileConfig(fileNameConfig)
              .withRollingPolicy(lakeSoulPolicy);
    }
  }

  private Object createWriter(Context sinkContext) {
    DataType PartitionColumns = FlinkUtil.getFields(DataType, false).stream()
        .filter(field -> !partitionKeyList.contains(field.getName()))
        .collect(Collectors.collectingAndThen(Collectors.toList(), LakeSoulTableSink::getDataType));
    if (bulkWriterFormat != null) {
      return bulkWriterFormat.createRuntimeEncoder(
          sinkContext, PartitionColumns);
    } else if (serializationFormat != null) {
      return new LakeSoulSchemaAdapter(
          serializationFormat.createRuntimeEncoder(
              sinkContext, PartitionColumns));
    } else {
      throw new TableException("Can not find format factory.");
    }
  }

  private LakeSoulCdcPartitionComputer partitionCdcComputer() {
    return new LakeSoulCdcPartitionComputer(
        DEFAULT_PARTITION_PATH,
        FlinkUtil.getFieldNames(DataType).toArray(new String[0]),
        FlinkUtil.getFieldDataTypes(DataType).toArray(new DataType[0]),
        partitionKeyList.toArray(new String[0]), FlinkUtil.isLakesoulCdcTable(flinkConf));
  }

  public static DataType getDataType(List<DataTypes.Field> fields) {
    return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
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
