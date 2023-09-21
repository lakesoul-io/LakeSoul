// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.table;

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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class LakeSoulTableSink implements DynamicTableSink, SupportsPartitioning,
        SupportsOverwrite, SupportsRowLevelDelete, SupportsRowLevelUpdate {

    private final String summaryName;
    private final String tableName;
    private final DataType dataType;
    private final ResolvedSchema schema;
    private final Configuration flinkConf;
    private final List<String> primaryKeyList;
    private final List<String> partitionKeyList;
    private boolean overwrite;

    public LakeSoulTableSink(String summaryName, String tableName, DataType dataType, List<String> primaryKeyList,
                             List<String> partitionKeyList, ReadableConfig flinkConf, ResolvedSchema schema) {
        this.summaryName = summaryName;
        this.tableName = tableName;
        this.primaryKeyList = primaryKeyList;
        this.schema = schema;
        this.partitionKeyList = partitionKeyList;
        this.flinkConf = (Configuration) flinkConf;
        this.dataType = dataType;
    }

    private LakeSoulTableSink(LakeSoulTableSink tableSink) {
        this.summaryName = tableSink.summaryName;
        this.tableName = tableSink.tableName;
        this.overwrite = tableSink.overwrite;
        this.dataType = tableSink.dataType;
        this.schema = tableSink.schema;
        this.flinkConf = tableSink.flinkConf;
        this.primaryKeyList = tableSink.primaryKeyList;
        this.partitionKeyList = tableSink.partitionKeyList;
    }

    private static LakeSoulTableSink createLakesoulTableSink(LakeSoulTableSink lts) {
        return new LakeSoulTableSink(lts);
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(ProviderContext providerContext,
                                                       DataStream<RowData> dataStream) {
                try {
                    return createStreamingSink(dataStream, context);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        if (flinkConf.getBoolean(USE_CDC)) {
            return ChangelogMode.upsert();
        } else if (this.primaryKeyList.isEmpty()) {
            return ChangelogMode.insertOnly();
        } else {
            return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.UPDATE_AFTER)
                    .build();
        }
    }

    /**
     * DataStream sink fileSystem and upload metadata
     */
    private DataStreamSink<?> createStreamingSink(DataStream<RowData> dataStream, Context sinkContext)
            throws IOException {
        Path path = FlinkUtil.makeQualifiedPath(new Path(flinkConf.getString(CATALOG_PATH)));
        int bucketParallelism = flinkConf.getInteger(HASH_BUCKET_NUM);
        //rowData key tools
        RowType rowType = (RowType) schema.toSourceRowDataType().notNull().getLogicalType();
        LakeSoulKeyGen keyGen = new LakeSoulKeyGen(rowType, primaryKeyList.toArray(new String[0]));
        //bucket file name config
        OutputFileConfig fileNameConfig = OutputFileConfig.builder().withPartSuffix(".parquet").build();
        //file rolling rule
        LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(flinkConf.getLong(FILE_ROLLING_SIZE),
                flinkConf.getLong(FILE_ROLLING_TIME));
        //redistribution by partitionKey
        dataStream = dataStream.partitionCustom(new HashPartitioner(), keyGen::getRePartitionHash);
        //rowData sink fileSystem Task
        LakeSoulMultiTablesSink<RowData> sink = LakeSoulMultiTablesSink.forOneTableBulkFormat(path,
                        new TableSchemaIdentity(new TableId(io.debezium.relational.TableId.parse(summaryName)), rowType,
                                path.toString(), primaryKeyList, partitionKeyList,
                                flinkConf.getBoolean(USE_CDC, false),
                                flinkConf.getString(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT)
                        ), flinkConf)
                .withBucketCheckInterval(flinkConf.getLong(BUCKET_CHECK_INTERVAL)).withRollingPolicy(rollingPolicy)
                .withOutputFileConfig(fileNameConfig).build();
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
    public RowLevelDeleteInfo applyRowLevelDelete(@Nullable RowLevelModificationScanContext context) {
        if (flinkConf.getBoolean(USE_CDC, false)) {
            flinkConf.set(DMLTYPE, DELETE_CDC);
        } else {
            flinkConf.set(DMLTYPE, DELETE);
        }

        return new LakeSoulRowLevelDelete();
    }

    @Override
    public RowLevelUpdateInfo applyRowLevelUpdate(List<Column> updatedColumns,
                                                  @Nullable RowLevelModificationScanContext context) {
        flinkConf.set(DMLTYPE, UPDATE);
        return new LakeSoulRowLevelUpdate();
    }

    private class LakeSoulRowLevelDelete implements RowLevelDeleteInfo {

        public Optional<List<Column>> requiredColumns() {
            return Optional.empty();
        }

        public SupportsRowLevelDelete.RowLevelDeleteMode getRowLevelDeleteMode() {
            if (flinkConf.getBoolean(USE_CDC, false)) {
                return RowLevelDeleteMode.DELETED_ROWS;
            } else {
                return RowLevelDeleteMode.REMAINING_ROWS;
            }
        }
    }

    private class LakeSoulRowLevelUpdate implements RowLevelUpdateInfo {
        public Optional<List<Column>> requiredColumns() {
            return Optional.empty();

        }

        public SupportsRowLevelUpdate.RowLevelUpdateMode getRowLevelUpdateMode() {
            if (primaryKeyList.isEmpty()) {
                return RowLevelUpdateMode.ALL_ROWS;
            } else {
                return SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS;
            }
        }
    }
}
