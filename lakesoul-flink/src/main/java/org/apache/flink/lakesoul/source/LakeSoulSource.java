// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.substrait.proto.Plan;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public abstract class LakeSoulSource<OUT> implements Source<OUT, LakeSoulPartitionSplit, LakeSoulPendingSplits> {
    final TableId tableId;

    protected final RowType projectedRowType;

    protected final RowType rowTypeWithPk;

    protected final boolean isBounded;

    protected final List<String> pkColumns;

    protected final List<String> partitionColumns;

    protected final Map<String, String> optionParams;

    @Nullable
    final List<Map<String, String>> remainingPartitions;

    @Nullable
    protected final Plan pushedFilter;


    @Nullable final Plan partitionFilters;
    protected final RowType tableRowType;

    public LakeSoulSource(TableId tableId,
                          RowType tableRowType,
                          RowType projectedRowType,
                          RowType rowTypeWithPk,
                          boolean isBounded,
                          List<String> pkColumns,
                          List<String> partitionColumns,
                          Map<String, String> optionParams,
                          @Nullable List<Map<String, String>> remainingPartitions,
                          @Nullable Plan pushedFilter,
                          @Nullable Plan partitionFilters
    ) {
        this.tableId = tableId;
        this.tableRowType = tableRowType;
        this.projectedRowType = projectedRowType;
        this.rowTypeWithPk = rowTypeWithPk;
        this.isBounded = isBounded;
        this.pkColumns = pkColumns;
        this.partitionColumns = partitionColumns;
        this.optionParams = optionParams;
        this.remainingPartitions = remainingPartitions;
        this.pushedFilter = pushedFilter;
        this.partitionFilters = partitionFilters;
    }
    public TableId getTableId(){
        return this.tableId;
    }

    @Override
    public Boundedness getBoundedness() {
        if (!this.isBounded) {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        } else {
            return Boundedness.BOUNDED;
        }
    }

    @Override
    public SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulPartitionSplit> enumContext) {
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(),
                tableId.schema());
        List<String> readStartTimestampWithTimeZone =
                Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_START_TIME(), ""),
                        optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        String readType = optionParams.getOrDefault(LakeSoulOptions.READ_TYPE(), "");
        if (getBoundedness().equals(Boundedness.CONTINUOUS_UNBOUNDED)) {
            return new LakeSoulAllPartitionDynamicSplitEnumerator(
                    enumContext,
                    new LakeSoulDynSplitAssigner(optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1")),
                    this.tableRowType,
                    Long.parseLong(optionParams.getOrDefault(LakeSoulOptions.DISCOVERY_INTERVAL(), "30000")),
                    convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                    tableInfo.getTableId(),
                    optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1"),
                    partitionColumns,
                    partitionFilters);

        } else {
            return staticSplitEnumerator(enumContext,
                    tableInfo,
                    readStartTimestampWithTimeZone,
                    readType);
        }
    }

    private LakeSoulStaticSplitEnumerator staticSplitEnumerator(
            SplitEnumeratorContext<LakeSoulPartitionSplit> enumContext,
            TableInfo tableInfo,
            List<String> readStartTimestampWithTimeZone,
            String readType) {
        List<String> readEndTimestampWithTimeZone =
                Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_END_TIME(), ""),
                        optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        List<DataFileInfo> dataFileInfoList;
        if (readType.equals("") || readType.equals("fullread")) {
            dataFileInfoList = Arrays.asList(getTargetDataFileInfo(tableInfo));
        } else {
            dataFileInfoList = new ArrayList<>();
            List<String> partDescs = new ArrayList<>();
            String partitionDescOpt = optionParams.getOrDefault(LakeSoulOptions.PARTITION_DESC(), "");
            if (partitionDescOpt.isEmpty() && remainingPartitions != null) {
                for (Map<String, String> part : remainingPartitions) {
                    String desc = DBUtil.formatPartitionDesc(part);
                    partDescs.add(desc);
                }
            } else {
                partDescs.add(partitionDescOpt);
            }
            for (String desc : partDescs) {
                dataFileInfoList.addAll(
                        Arrays.asList(DataOperation.getIncrementalPartitionDataInfo(tableInfo.getTableId(),
                                desc,
                                convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                                convertTimeFormatWithTimeZone(readEndTimestampWithTimeZone),
                                readType)));
            }
        }
        int capacity = 100;
        ArrayList<LakeSoulPartitionSplit> splits = new ArrayList<>(capacity);
        if (!FlinkUtil.isExistHashPartition(tableInfo)) {
            for (DataFileInfo dataFileInfo : dataFileInfoList) {
                ArrayList<Path> tmp = new ArrayList<>();
                tmp.add(new Path(dataFileInfo.path()));
                splits.add(new LakeSoulPartitionSplit(String.valueOf(dataFileInfo.hashCode()),
                        tmp,
                        0,
                        dataFileInfo.range_partitions()));
            }
        } else {
            Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                    FlinkUtil.splitDataInfosToRangeAndHashPartition(tableInfo,
                            dataFileInfoList.toArray(new DataFileInfo[0]));
            for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                    splits.add(new LakeSoulPartitionSplit(String.valueOf(split.hashCode()),
                            split.getValue(),
                            0,
                            entry.getKey()));
                }
            }
        }
        return new LakeSoulStaticSplitEnumerator(enumContext,
                new LakeSoulSimpleSplitAssigner(splits));
    }


    private DataFileInfo[] getTargetDataFileInfo(TableInfo tableInfo) {
        return FlinkUtil.getTargetDataFileInfo(tableInfo,
                this.remainingPartitions);
    }

    private long convertTimeFormatWithTimeZone(List<String> readTimestampWithTimeZone) {
        String time = readTimestampWithTimeZone.get(0);
        String timeZone = readTimestampWithTimeZone.get(1);
        if (timeZone.equals("") || !Arrays.asList(TimeZone.getAvailableIDs()).contains(timeZone)) {
            timeZone = TimeZone.getDefault().getID();
        }
        long readTimeStamp = 0;
        if (!time.equals("")) {
            readTimeStamp = LocalDateTime.parse(time,
                            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    .atZone(ZoneId.of(timeZone)).toInstant().toEpochMilli();
        }
        return readTimeStamp;
    }

    @Override
    public SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulPartitionSplit> enumContext,
            LakeSoulPendingSplits checkpoint) throws Exception {
        return new LakeSoulAllPartitionDynamicSplitEnumerator(
                enumContext,
                new LakeSoulDynSplitAssigner(checkpoint.getSplits(),
                        String.valueOf(checkpoint.getHashBucketNum())),
                this.tableRowType,
                checkpoint.getDiscoverInterval(),
                checkpoint.getLastReadTimestamp(),
                checkpoint.getTableId(),
                String.valueOf(checkpoint.getHashBucketNum()),
                this.partitionColumns,
                this.partitionFilters
        );
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPartitionSplit> getSplitSerializer() {
        return new SimpleLakeSoulSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPendingSplits> getEnumeratorCheckpointSerializer() {
        return new SimpleLakeSoulPendingSplitsSerializer();
    }
}
