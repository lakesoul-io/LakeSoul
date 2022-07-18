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

package org.apache.flink.lakeSoul.table;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.lakeSoul.metaData.DataFileMetaData;
import org.apache.flink.lakeSoul.sink.LakSoulFileWriter;
import org.apache.flink.lakeSoul.sink.FileSinkFunction;
import org.apache.flink.lakeSoul.sink.MetaDataCommit;
import org.apache.flink.lakeSoul.sink.bucket.FlinkBucketAssigner;
import org.apache.flink.lakeSoul.sink.bucket.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakeSoul.sink.bucket.bulkFormat.ProjectionBulkFactory;
import org.apache.flink.lakeSoul.sink.partition.DataPartitioner;
import org.apache.flink.lakeSoul.sink.partition.CdcPartitionComputer;
import org.apache.flink.lakeSoul.tools.FlinkUtil;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.lakeSoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.api.common.serialization.BulkWriter;
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

import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.BUCKET_CHECK_INTERVAL;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.CATALOG_PATH;
import static org.apache.flink.lakeSoul.tools.LakeSoulKeyGen.DEFAULT_PARTITION_PATH;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.FILE_ROLLING_SIZE;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.FILE_ROLLING_TIME;
import static org.apache.flink.lakeSoul.tools.LakeSoulSinkOptions.USE_CDC;

public class LakeSoulTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
  private EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
  private boolean overwrite;
  private DataType dataType;
  private ResolvedSchema schema;
  private Configuration flinkConf;
  private List<String> partitionKeyList;

  private static LakeSoulTableSink createLakesoulTableSink(LakeSoulTableSink lts) {
    return new LakeSoulTableSink(lts);
  }

  public LakeSoulTableSink(
      DataType dataType,
      List<String> partitionKeyList,
      ReadableConfig flinkConf,
      EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
      ResolvedSchema schema
  ) {
    this.bulkWriterFormat = bulkWriterFormat;
    this.schema = schema;
    this.partitionKeyList = partitionKeyList;
    this.flinkConf = (Configuration) flinkConf;
    this.dataType = dataType;
  }

  private LakeSoulTableSink(LakeSoulTableSink tableSink) {
    this.overwrite = tableSink.overwrite;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    return (DataStreamSinkProvider) (dataStream) -> createStreamingSink(dataStream, context);
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
                                                Context sinkContext) {
    Path path = new Path(flinkConf.getString(CATALOG_PATH));
    int bucketParallelism = flinkConf.getInteger(BUCKET_PARALLELISM);
    //rowData key tools
    LakeSoulKeyGen keyGen = new LakeSoulKeyGen((RowType) schema.toSourceRowDataType().notNull().getLogicalType(),
        flinkConf, partitionKeyList);
    //bucket file name config
    OutputFileConfig fileNameConfig = OutputFileConfig.builder().build();
    //if use cdc  add rowKind column
    CdcPartitionComputer partitionComputer = partitionCdcComputer(flinkConf.getBoolean(USE_CDC));
    //partition Data distribution rule
    FlinkBucketAssigner assigner = new FlinkBucketAssigner(partitionComputer);
    //file rolling rule
    LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(
        flinkConf.getLong(FILE_ROLLING_SIZE), flinkConf.getLong(FILE_ROLLING_TIME), keyGen);
    //redistribution by partitionKey
    dataStream = dataStream.partitionCustom(new DataPartitioner<>(), keyGen::getRePartitionKey);

    //rowData sink fileSystem Task
    LakSoulFileWriter<RowData> lakSoulFileWriter =
        new LakSoulFileWriter<>(flinkConf.getLong(BUCKET_CHECK_INTERVAL),
            //create sink Bulk format
            FileSinkFunction.forBulkFormat(
                    path,
                    new ProjectionBulkFactory(
                        (BulkWriter.Factory<RowData>) getParquetFormat(sinkContext),
                        partitionComputer))
                .withBucketAssigner(assigner)
                .withOutputFileConfig(fileNameConfig)
                .withRollingPolicy(rollingPolicy),
            partitionKeyList, flinkConf, fileNameConfig);

    DataStream<DataFileMetaData> writeResultStream =
        dataStream.transform(LakSoulFileWriter.class.getSimpleName(),
                TypeInformation.of(DataFileMetaData.class),
                lakSoulFileWriter).name("DataWrite")
            .setParallelism(bucketParallelism);

    //metadata upload Task
    DataStream<Void> commitStream = writeResultStream.transform(
            MetaDataCommit.class.getSimpleName(),
            Types.VOID,
            new MetaDataCommit(path, flinkConf))
        .setParallelism(1).name("DataCommit")
        .setMaxParallelism(1);

    return commitStream.addSink(new DiscardingSink<>())
        .name("end")
        .setParallelism(1);
  }

  /*
   * create parquet data type
   */
  private Object getParquetFormat(Context sinkContext) {
    DataType resultType = FlinkUtil.getFields(dataType, flinkConf.getBoolean(USE_CDC)).stream()
        .filter(field -> !partitionKeyList.contains(field.getName()))
        .collect(Collectors.collectingAndThen(Collectors.toList(), LakeSoulTableSink::getDataType));
    if (bulkWriterFormat != null) {
      return bulkWriterFormat.createRuntimeEncoder(
          sinkContext, resultType);
    } else {
      throw new TableException("bulk write format is null");
    }
  }

  /*
   * if cdc table  add rowKind mark column
   */
  private CdcPartitionComputer partitionCdcComputer(boolean useCdc) {
    return new CdcPartitionComputer(
        DEFAULT_PARTITION_PATH,
        FlinkUtil.getFieldNames(dataType).toArray(new String[0]),
        FlinkUtil.getFieldDataTypes(dataType).toArray(new DataType[0]),
        partitionKeyList.toArray(new String[0]),
        useCdc);
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
