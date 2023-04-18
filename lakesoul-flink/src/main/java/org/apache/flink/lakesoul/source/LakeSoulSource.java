package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.*;
import java.util.stream.Collectors;

public class LakeSoulSource implements Source<RowData, LakeSoulSplit, LakeSoulPendingSplits> {
    TableId tableId;
    RowType rowType;
    boolean isStreaming;
    List<String> pkColumns;

    List<Map<String, String>> remainingPartitions;

    public LakeSoulSource(TableId tableId, RowType rowType, boolean isStreaming, List<String> pkColumns, List<Map<String, String>> remainingPartitions) {
        this.tableId = tableId;
        this.rowType = rowType;
        this.isStreaming = isStreaming;
        this.pkColumns = pkColumns;
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
            return new LakeSoulSplitReader(readerContext.getConfiguration(), this.rowType, this.pkColumns);
        }, new LakeSoulRecordEmitter(), readerContext.getConfiguration(), readerContext);
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> createEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext) throws Exception {
        TableInfo tif = DataOperation.dbManager().getTableInfoByNameAndNamespace(tableId.table(), tableId.schema());
        if (this.isStreaming) {
            return new LakeSoulDynamicSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(), 1000, 0, tif.getTableId(), "");
        } else {
            DataFileInfo[] dfinfos = getTargetDataFileInfo(tif);
            int capacity = 100;
            ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
            int i = 0;
            Map<String, Map<String, List<Path>>> splitByRangeAndHashPartition = FlinkUtil.splitDataInfosToRangeAndHashPartition(dfinfos);
            for (Map.Entry<String, Map<String, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                for (Map.Entry<String, List<Path>> split : entry.getValue().entrySet()) {
                    splits.add(new LakeSoulSplit(i + "", split.getValue(),0));
                }
            }
            return new LakeSoulStaticSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(splits));
        }
    }

    private DataFileInfo[] getTargetDataFileInfo(TableInfo tif) throws Exception {
        return FlinkUtil.getTargetDataFileInfo(tif, this.remainingPartitions);
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
