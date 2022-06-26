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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.metaData.DataInfo;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulBucketsBuilder;
import org.apache.flink.lakesoul.sink.fileSystem.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakesoul.sink.partition.BucketPartitioner;
import org.apache.flink.lakesoul.sink.partition.LakesoulCdcPartitionComputer;
import org.apache.flink.lakesoul.sink.fileSystem.FlinkBucketAssigner;
import org.apache.flink.lakesoul.table.LakesoulSchemaAdapter;
import org.apache.flink.lakesoul.tools.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.filesystem.FileSystemFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.tools.LakeSoulKeyGen.DEFAULT_PARTITION_PATH;
import static org.apache.flink.lakesoul.tools.LakeSoulSinkOptions.CATALOG_PATH;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

public class LakesoulTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
    private boolean overwrite = false;
    private ObjectIdentifier tableIdentifier;
    private Configuration tableOptions;
    private DataType physicalRowDataType;
    private Path path;
    private LakeSoulKeyGen keyGen;
    private ResolvedSchema schema;

    List<String> partitionKeys;

    private EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat;
    private EncodingFormat<SerializationSchema<RowData>> serializationFormat;


    @Override
    public boolean requiresPartitionGrouping(boolean supportsGrouping) {
        return false;
    }

    public LakesoulTableSink(ObjectIdentifier tableIdentifier,
                             DataType physicalRowDataType,
                             List<String> partitionKeys,
                             ReadableConfig tableOptions,
                             EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat,
                             EncodingFormat<SerializationSchema<RowData>> serializationFormat,
                             ResolvedSchema schema
    ) {
        this.bulkWriterFormat = bulkWriterFormat;
        this.serializationFormat = serializationFormat;
        this.tableIdentifier = tableIdentifier;
        this.partitionKeys = partitionKeys;
        this.tableOptions = (Configuration) tableOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.schema = schema;
    }

    private LakesoulTableSink(LakesoulTableSink lts) {
        this.overwrite = lts.overwrite;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : changelogMode.getContainedKinds()) {
            builder.addContainedKind(kind);
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        return (DataStreamSinkProvider) (dataStream) -> consume(dataStream, sinkContext);
    }

    private DataStreamSink<?> consume(DataStream<RowData> dataStream, Context sinkContext) {
        final int parallelism = dataStream.getParallelism();
        return createStreamingSink(dataStream, sinkContext, parallelism);

    }

    private DataStreamSink<?> createStreamingSink(
            DataStream<RowData> dataStream,
            Context sinkContext,
            final int parallelism) {
        FileSystemFactory fsFactory = FileSystem::get;
        LakesoulCdcPartitionComputer computer = partitionCdcComputer();
        Object writer = createWriter(sinkContext, tableOptions);
        boolean isEncoder = writer instanceof Encoder;

        keyGen = new LakeSoulKeyGen((RowType) schema.toSourceRowDataType().notNull().getLogicalType(), tableOptions);
        FlinkBucketAssigner assigner = new FlinkBucketAssigner(computer);
        keyGen.partitionKey = partitionKeys;
        LakeSoulRollingPolicyImpl lakesoulPolicy = new LakeSoulRollingPolicyImpl(!isEncoder);
        lakesoulPolicy.setKeyGen(keyGen);
        OutputFileConfig.OutputFileConfigBuilder fileNamingBuilder = OutputFileConfig.builder();
        OutputFileConfig fileNamingConfig = fileNamingBuilder.build();
        this.path = new Path(tableOptions.getString(CATALOG_PATH));

        LakeSoulBucketsBuilder<RowData, String, ? extends LakeSoulBucketsBuilder<RowData, ?, ?>> bucketsBuilder;
        if (isEncoder) {
            //noinspection unchecked
            bucketsBuilder =
                    LakesoulFileSink.forRowFormat(
                                    this.path,
                                    new ProjectionEncoder((Encoder<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(lakesoulPolicy);
        } else {
            //noinspection unchecked
            bucketsBuilder =
                    LakesoulFileSink.forBulkFormat(
                                    this.path,
                                    new ProjectionBulkFactory(
                                            (BulkWriter.Factory<RowData>) writer, computer))
                            .withBucketAssigner(assigner)
                            .withOutputFileConfig(fileNamingConfig)
                            .withRollingPolicy(lakesoulPolicy);
        }
        long bucketCheckInterval = Duration.ofMinutes(1).toMillis();
        BucketPartitioner<String> partitioner = new BucketPartitioner<>();

        dataStream = dataStream.partitionCustom(partitioner, keyGen::getBucketPartitionKey);

        DataStream<DataInfo> writerStream = LakesoulSink.writer(
                bucketCheckInterval, dataStream, bucketsBuilder,
                fileNamingConfig, parallelism, partitionKeys, tableOptions);

        return LakesoulSink.sink(
                writerStream, this.path, tableIdentifier, partitionKeys, fsFactory, tableOptions);
    }

    private LakesoulCdcPartitionComputer partitionCdcComputer() {
        return new LakesoulCdcPartitionComputer(
                DEFAULT_PARTITION_PATH,
                getFieldNames(physicalRowDataType).toArray(new String[0]),
                getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                partitionKeys.toArray(new String[0]), FlinkUtil.isLakesoulCdcTable(tableOptions));
    }

    @Override
    public DynamicTableSink copy() {
        return new LakesoulTableSink(this);
    }

    @Override
    public String asSummaryString() {
        return "lakesoul table sink";
    }

    @Override
    public void applyOverwrite(boolean newOverwrite) {
        this.overwrite = newOverwrite;
    }

    @Override
    public void applyStaticPartition(Map<String, String> map) {

    }

    private Object createWriter(Context sinkContext, Configuration tableOptions) {
        DataType physicalDataTypeWithoutPartitionColumns = getFields(physicalRowDataType, FlinkUtil.isLakesoulCdcTable(tableOptions)).stream()
                .filter(field -> !partitionKeys.contains(field.getName()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), LakesoulTableSink::ROW));
        if (bulkWriterFormat != null) {
            return bulkWriterFormat.createRuntimeEncoder(
                    sinkContext, physicalDataTypeWithoutPartitionColumns);
        } else if (serializationFormat != null) {
            return new LakesoulSchemaAdapter(
                    serializationFormat.createRuntimeEncoder(
                            sinkContext, physicalDataTypeWithoutPartitionColumns));
        } else {
            throw new TableException("Can not find format factory.");
        }
    }

    public static DataType ROW(List<DataTypes.Field> fields) {
        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
    }

    public static List<String> getFieldNames(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
            return getFieldNames(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return LogicalTypeChecks.getFieldNames(type);
        }
        return Collections.emptyList();
    }

    public static List<DataType> getFieldDataTypes(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
            return getFieldDataTypes(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return dataType.getChildren();
        }
        return Collections.emptyList();
    }

    public static List<DataTypes.Field> getFields(DataType dataType, Boolean isCdc) {
        final List<String> names = getFieldNames(dataType);
        final List<DataType> dataTypes = getFieldDataTypes(dataType);
        if (isCdc) {
            //todo MetaCommon.LakesoulCdcColumnName()
            names.add("MetaCommon.LakesoulCdcColumnName()");
            dataTypes.add(DataTypes.VARCHAR(30));
        }
        return IntStream.range(0, names.size())
                .mapToObj(i -> DataTypes.FIELD(names.get(i), dataTypes.get(i)))
                .collect(Collectors.toList());
    }

    private static class ProjectionEncoder implements Encoder<RowData> {

        private final Encoder<RowData> encoder;
        private final LakesoulCdcPartitionComputer computer;

        private ProjectionEncoder(Encoder<RowData> encoder, LakesoulCdcPartitionComputer computer) {
            this.encoder = encoder;
            this.computer = computer;
        }

        @Override
        public void encode(RowData element, OutputStream stream) throws IOException {
            encoder.encode(computer.projectColumnsToWrite(element), stream);
        }
    }

    public static class ProjectionBulkFactory implements BulkWriter.Factory<RowData> {

        private final BulkWriter.Factory<RowData> factory;
        private final LakesoulCdcPartitionComputer computer;

        public ProjectionBulkFactory(
                BulkWriter.Factory<RowData> factory, LakesoulCdcPartitionComputer computer) {
            this.factory = factory;
            this.computer = computer;
        }

        @Override
        public BulkWriter<RowData> create(FSDataOutputStream out) throws IOException {
            BulkWriter<RowData> writer = factory.create(out);
            return new BulkWriter<RowData>() {

                @Override
                public void addElement(RowData element) throws IOException {
                    writer.addElement(computer.projectColumnsToWrite(element));
                }

                @Override
                public void flush() throws IOException {
                    writer.flush();
                }

                @Override
                public void finish() throws IOException {
                    writer.finish();
                }
            };
        }
    }
}


