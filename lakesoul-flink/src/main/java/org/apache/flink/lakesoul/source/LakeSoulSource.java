package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
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
        DataFileInfo[] dfinfos = getTargetDataFileInfo(tif);
        int capacity = 100;
        ArrayList<LakeSoulSplit> splits = new ArrayList<>(capacity);
        int i = 0;
        Map<String, Map<String, List<Path>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        for (DataFileInfo pif : dfinfos) {
            // todo : add procession of no hashPartition
            if (pif.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(String.valueOf(pif.file_bucket_id()), v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent("-1", v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            }
        }
        for (Map.Entry<String, Map<String, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
            for (Map.Entry<String, List<Path>> split : entry.getValue().entrySet()) {
                splits.add(new LakeSoulSplit(i + "", split.getValue()));
            }
        }
        return new LakeSoulStaticSplitEnumerator(enumContext, new LakeSoulSimpleSplitAssigner(splits));
    }

    private DataFileInfo[] getTargetDataFileInfo(TableInfo tif) throws Exception {
        if (remainingPartitions == null || remainingPartitions.size() == 0) {
            return DataOperation.getTableDataInfo(tif.getTableId());
        } else {
            List<String> partitionDescs = remainingPartitions.stream()
                    .map(map -> map.entrySet().stream()
                            .map(entry -> entry.getKey() + "=" + entry.getValue())
                            .collect(Collectors.joining(",")))
                    .collect(Collectors.toList());
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (String partitionDesc : partitionDescs) {
                partitionInfos.add(MetaVersion.getSinglePartitionInfo(tif.getTableId(), partitionDesc, ""));
            }
            PartitionInfo[] ptinfos = partitionInfos.toArray(new PartitionInfo[partitionInfos.size()]);
            return DataOperation.getTableDataInfo(ptinfos);
        }
    }

    @Override
    public SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> restoreEnumerator(
            SplitEnumeratorContext<LakeSoulSplit> enumContext, LakeSoulPendingSplits checkpoint) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulSplit> getSplitSerializer() {
        return new SimpleLakeSoulSerializer();
    }

    @Override
    public SimpleVersionedSerializer<LakeSoulPendingSplits> getEnumeratorCheckpointSerializer() {
        return null;
    }
}
