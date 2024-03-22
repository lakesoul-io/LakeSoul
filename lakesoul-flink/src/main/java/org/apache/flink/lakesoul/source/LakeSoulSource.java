// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class LakeSoulSource implements Source<RowData, LakeSoulSplit, LakeSoulPendingSplits> {
    TableId tableId;

    RowType rowType;

    RowType rowTypeWithPk;

    boolean isStreaming;

    List<String> pkColumns;

    Map<String, String> optionParams;

    @Nullable
    List<Map<String, String>> remainingPartitions;

    @Nullable
    FilterPredicate filter;

    public LakeSoulSource(TableId tableId,
                          RowType rowType,
                          RowType rowTypeWithPk,
                          boolean isStreaming,
                          List<String> pkColumns,
                          Map<String, String> optionParams,
                          @Nullable List<Map<String, String>> remainingPartitions,
                          @Nullable FilterPredicate filter) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.rowTypeWithPk = rowTypeWithPk;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
        this.optionParams = optionParams;
        this.remainingPartitions = remainingPartitions;
        this.filter = filter;
    }

    @Override
    public Boundedness getBoundedness() {
        if (this.isStreaming) {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        } else {
            return Boundedness.BOUNDED;
        }
    }

    @Override
    public SourceReader<RowData, LakeSoulSplit> createReader(SourceReaderContext readerContext) throws Exception {
        Configuration conf = Configuration.fromMap(optionParams);
        conf.addAll(readerContext.getConfiguration());
        return new LakeSoulSourceReader(
                () -> new LakeSoulSplitReader(conf,
                        this.rowType,
                        this.rowTypeWithPk,
                        this.pkColumns,
                        this.isStreaming,
                        this.optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, ""),
                        this.filter),
                new LakeSoulRecordEmitter(),
                readerContext.getConfiguration(),
                readerContext);
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) {
        TableInfo tableInfo = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(),
                tableId.schema());
        List<String> readStartTimestampWithTimeZone =
                Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_START_TIME(), ""),
                        optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        String readType = optionParams.getOrDefault(LakeSoulOptions.READ_TYPE(), "");
        if (this.isStreaming) {
            String partDesc = optionParams.getOrDefault(LakeSoulOptions.PARTITION_DESC(), "");
            if (partDesc.isEmpty()) {
                if (remainingPartitions != null && !remainingPartitions.isEmpty()) {
                    // use remaining partition
                    if (remainingPartitions.size() > 1) {
                        throw new RuntimeException("Streaming read allows only one specified partition," +
                                " or no specified partition to incrementally read entire table");
                    }
                    partDesc = DBUtil.formatPartitionDesc(remainingPartitions.get(0));
                }
            }
            return new LakeSoulDynamicSplitEnumerator(enumContext,
                    new LakeSoulDynSplitAssigner(optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1")),
                    Long.parseLong(optionParams.getOrDefault(LakeSoulOptions.DISCOVERY_INTERVAL(), "30000")),
                    convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                    tableInfo.getTableId(),
                    partDesc,
                    optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1"));
        } else {
            return staticSplitEnumerator(enumContext,
                    tableInfo,
                    readStartTimestampWithTimeZone,
                    readType);
        }
    }

    private LakeSoulStaticSplitEnumerator staticSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> enumContext,
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
                dataFileInfoList.addAll(Arrays.asList(DataOperation.getIncrementalPartitionDataInfo(tableInfo.getTableId(),
                        desc,
                        convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                        convertTimeFormatWithTimeZone(readEndTimestampWithTimeZone),
                        readType)));
            }
        }
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        if (!FlinkUtil.isExistHashPartition(tableInfo)) {
            for (DataFileInfo dataFileInfo : dataFileInfoList) {
                ArrayList<Path> tmp = new ArrayList<>();
                tmp.add(new Path(dataFileInfo.path()));
                splits.add(new LakeSoulSplit(String.valueOf(dataFileInfo.hashCode()),
                        tmp,
                        0));
            }
        } else {
            Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                    FlinkUtil.splitDataInfosToRangeAndHashPartition(tableInfo.getTableId(),
                            dataFileInfoList.toArray(new DataFileInfo[0]));
            for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                    splits.add(new LakeSoulSplit(String.valueOf(split.hashCode()),
                            split.getValue(),
                            0));
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
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext,
            LakeSoulPendingSplits checkpoint) throws Exception {
        return new LakeSoulDynamicSplitEnumerator(enumContext,
                new LakeSoulDynSplitAssigner(checkpoint.getSplits(),
                        String.valueOf(checkpoint.getHashBucketNum())),
                checkpoint.getDiscoverInterval(),
                checkpoint.getLastReadTimestamp(),
                checkpoint.getTableid(),
                checkpoint.getParDesc(),
                String.valueOf(checkpoint.getHashBucketNum()));
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulSplit> getSplitSerializer() {
        return new SimpleLakeSoulSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPendingSplits> getEnumeratorCheckpointSerializer() {
        return new SimpleLakeSoulPendingSplitsSerializer();
    }
}
