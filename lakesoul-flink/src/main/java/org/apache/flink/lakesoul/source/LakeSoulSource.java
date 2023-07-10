/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.source;

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
    List<Map<String, String>> remainingPartitions;

    public LakeSoulSource(TableId tableId, RowType rowType, RowType rowTypeWithPk, boolean isStreaming,
                          List<String> pkColumns, Map<String, String> optionParams,
                          List<Map<String, String>> remainingPartitions) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.rowTypeWithPk = rowTypeWithPk;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
        this.optionParams = optionParams;
        this.remainingPartitions = remainingPartitions;
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
                () -> new LakeSoulSplitReader(conf, this.rowType, this.rowTypeWithPk, this.pkColumns, this.isStreaming,
                        this.optionParams.getOrDefault(LakeSoulSinkOptions.CDC_CHANGE_COLUMN, "")),
                new LakeSoulRecordEmitter(), readerContext.getConfiguration(), readerContext);
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) {
        TableInfo tif = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        List<String> readStartTimestampWithTimeZone =
                Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_START_TIME(), ""),
                        optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        String readType = optionParams.getOrDefault(LakeSoulOptions.READ_TYPE(), "");
        if (this.isStreaming) {
            return new LakeSoulDynamicSplitEnumerator(enumContext,
                    new LakeSoulDynSplitAssigner(optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1")),
                    Long.parseLong(optionParams.getOrDefault(LakeSoulOptions.DISCOVERY_INTERVAL(), "30000")),
                    convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone), tif.getTableId(),
                    optionParams.getOrDefault(LakeSoulOptions.PARTITION_DESC(), ""),
                    optionParams.getOrDefault(LakeSoulOptions.HASH_BUCKET_NUM(), "-1"));
        } else {
            return staticSplitEnumerator(enumContext, tif, readStartTimestampWithTimeZone, readType);
        }
    }

    private LakeSoulStaticSplitEnumerator staticSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> enumContext,
                                                                TableInfo tif,
                                                                List<String> readStartTimestampWithTimeZone,
                                                                String readType) {
        List<String> readEndTimestampWithTimeZone =
                Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_END_TIME(), ""),
                        optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        DataFileInfo[] dfinfos;
        if (readType.equals("") || readType.equals("fullread")) {
            dfinfos = getTargetDataFileInfo(tif);
        } else {
            dfinfos = DataOperation.getIncrementalPartitionDataInfo(tif.getTableId(),
                    optionParams.getOrDefault(LakeSoulOptions.PARTITION_DESC(), ""),
                    convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                    convertTimeFormatWithTimeZone(readEndTimestampWithTimeZone), readType);
        }
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        if (!FlinkUtil.isExistHashPartition(tif)) {
            for (DataFileInfo dfinfo : dfinfos) {
                ArrayList<Path> tmp = new ArrayList<>();
                tmp.add(new Path(dfinfo.path()));
                splits.add(new LakeSoulSplit(String.valueOf(dfinfo.hashCode()), tmp, 0));
            }
        } else {
            Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                    FlinkUtil.splitDataInfosToRangeAndHashPartition(tif.getTableId(), dfinfos);
            for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                    splits.add(new LakeSoulSplit(String.valueOf(split.hashCode()), split.getValue(), 0));
                }
            }
        }
        return new LakeSoulStaticSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(splits));
    }


    private DataFileInfo[] getTargetDataFileInfo(TableInfo tif) {
        return FlinkUtil.getTargetDataFileInfo(tif, this.remainingPartitions);
    }

    private long convertTimeFormatWithTimeZone(List<String> readTimestampWithTimeZone) {
        String time = readTimestampWithTimeZone.get(0);
        String timeZone = readTimestampWithTimeZone.get(1);
        if (timeZone.equals("") || !Arrays.asList(TimeZone.getAvailableIDs()).contains(timeZone)) {
            timeZone = TimeZone.getDefault().getID();
        }
        long readTimeStamp = 0;
        if (!time.equals("")) {
            readTimeStamp = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    .atZone(ZoneId.of(timeZone)).toInstant().toEpochMilli();
        }
        return readTimeStamp;
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext, LakeSoulPendingSplits checkpoint) throws Exception {
        return new LakeSoulDynamicSplitEnumerator(enumContext,
                new LakeSoulDynSplitAssigner(checkpoint.getSplits(), String.valueOf(checkpoint.getHashBucketNum())),
                checkpoint.getDiscoverInterval(), checkpoint.getLastReadTimestamp(), checkpoint.getTableid(),
                checkpoint.getParDesc(), String.valueOf(checkpoint.getHashBucketNum()));
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
