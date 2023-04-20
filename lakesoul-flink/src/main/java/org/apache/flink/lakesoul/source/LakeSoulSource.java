package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
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

    public LakeSoulSource(TableId tableId, RowType rowType, RowType rowTypeWithPk, boolean isStreaming, List<String> pkColumns, Map<String, String> optionParams, List<Map<String, String>> remainingPartitions) {
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
        return new LakeSoulSourceReader(() -> {
            return new LakeSoulSplitReader(readerContext.getConfiguration(), this.rowType, this.rowTypeWithPk, this.pkColumns, null != this.remainingPartitions && this.remainingPartitions.size() == 0);
        }, new LakeSoulRecordEmitter(), readerContext.getConfiguration(), readerContext);
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) throws Exception {
        TableInfo tif = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        List<String> readStartTimestampWithTimeZone = Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_START_TIME(), ""),
                optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
        if (this.isStreaming) {
            return new LakeSoulDynamicSplitEnumerator(
                    enumContext,
                    new LakeSoulSimpleSplitAssigner(),
                    Long.parseLong(optionParams.getOrDefault(LakeSoulOptions.DISCOVERY_INTERVAL(), "10000")),
                    convertTimeFormatWithTimeZone(readStartTimestampWithTimeZone),
                    tif.getTableId(),
                    optionParams.getOrDefault(LakeSoulOptions.PARTITION_DESC(), "-5")
            );
        } else {
            List<String> readEndTimestampWithTimeZone = Arrays.asList(optionParams.getOrDefault(LakeSoulOptions.READ_END_TIME(), ""),
                    optionParams.getOrDefault(LakeSoulOptions.TIME_ZONE(), ""));
            DataFileInfo[] dfinfos = getTargetDataFileInfo(tif);
            int capacity = 100;
            ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
            int i = 0;
            Map<String, Map<String, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(tif.getTableId(), dfinfos);
            if (FlinkUtil.isExistHashPartition(tif)) {
                for (Map.Entry<String, Map<String, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                    for (Map.Entry<String, List<Path>> split : entry.getValue().entrySet()) {
                        splits.add(new LakeSoulSplit(i + "", split.getValue(), 0));
                    }
                }
            } else {
                for (DataFileInfo dfinfo : dfinfos) {
                    ArrayList<Path> tmp = new ArrayList<>();
                    tmp.add(new Path(dfinfo.path()));
                    splits.add(new LakeSoulSplit(i + "", tmp, 0));
                }
            }
            return new LakeSoulStaticSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(splits));
        }
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
            readTimeStamp = LocalDateTime.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).atZone(ZoneId.of(timeZone)).toInstant().toEpochMilli();
        }
        return readTimeStamp;
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext, LakeSoulPendingSplits checkpoint) throws Exception {
        return new LakeSoulDynamicSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(checkpoint.getSplits()), checkpoint.getDiscoverInterval(), checkpoint.getLastReadTimestamp(), checkpoint.getTableid(), checkpoint.getParDesc());
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
