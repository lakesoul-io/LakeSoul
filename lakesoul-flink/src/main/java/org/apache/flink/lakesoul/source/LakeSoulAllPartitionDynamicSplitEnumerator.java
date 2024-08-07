// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LakeSoulAllPartitionDynamicSplitEnumerator implements SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulAllPartitionDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulPartitionSplit> context;

    private final LakeSoulDynSplitAssigner splitAssigner;
    private final long discoveryInterval;
    private final Map<String, Long> partitionLatestTimestamp;
    private final Set<Integer> taskIdsAwaitingSplit;
    private final Plan partitionFilters;
    private final List<String> partitionColumns;
    private final TableInfo tableInfo;
    String tableId;
    private long startTime;
    private long nextStartTime;
    private int hashBucketNum = -1;

    protected Schema partitionArrowSchema;

    public LakeSoulAllPartitionDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulPartitionSplit> context,
                                                      LakeSoulDynSplitAssigner splitAssigner,
                                                      RowType rowType,
                                                      long discoveryInterval,
                                                      long startTime,
                                                      String tableId,
                                                      String hashBucketNum,
                                                      List<String> partitionColumns,
                                                      Plan partitionFilters) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.discoveryInterval = discoveryInterval;
        this.tableId = tableId;
        this.startTime = startTime;
        this.hashBucketNum = Integer.parseInt(hashBucketNum);
        this.taskIdsAwaitingSplit = Sets.newConcurrentHashSet();
        this.partitionLatestTimestamp = Maps.newConcurrentMap();
        this.partitionColumns = partitionColumns;

        Schema tableSchema = ArrowUtils.toArrowSchema(rowType);
        List<Field> partitionFields = partitionColumns.stream().map(tableSchema::findField).collect(Collectors.toList());

        this.partitionArrowSchema = new Schema(partitionFields);
        this.partitionFilters = partitionFilters;
        tableInfo = DataOperation.dbManager().getTableInfoByTableId(tableId);
    }

    @Override
    public void start() {
        context.callAsync(this::enumerateSplits, this::processDiscoveredSplits, discoveryInterval,
                discoveryInterval);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        int tasksSize = context.registeredReaders().size();
        Optional<LakeSoulPartitionSplit> nextSplit = this.splitAssigner.getNext(subtaskId, tasksSize);
        if (nextSplit.isPresent()) {
            context.assignSplit(nextSplit.get(), subtaskId);
            taskIdsAwaitingSplit.remove(subtaskId);
        } else {
            taskIdsAwaitingSplit.add(subtaskId);
        }

    }

    @Override
    public void addSplitsBack(List<LakeSoulPartitionSplit> splits, int subtaskId) {
        LOG.info("Add split back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        LakeSoulPendingSplits pendingSplits =
                new LakeSoulPendingSplits(splitAssigner.remainingSplits(), this.nextStartTime, this.tableId, "",
                        this.discoveryInterval, this.hashBucketNum);
        LOG.info("LakeSoulAllPartitionDynamicSplitEnumerator snapshotState {}", pendingSplits);
        return pendingSplits;
    }

    @Override
    public void close() throws IOException {

    }

    private void processDiscoveredSplits(Collection<LakeSoulPartitionSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files", error);
            return;
        }
        LOG.info("Process discovered splits {}", splits);
        int tasksSize = context.registeredReaders().size();
        this.splitAssigner.addSplits(splits);
        for (Integer item : taskIdsAwaitingSplit) {
            Optional<LakeSoulPartitionSplit> al = this.splitAssigner.getNext(item, tasksSize);
            if (al.isPresent()) {
                context.assignSplit(al.get(), item);
                taskIdsAwaitingSplit.remove(item);
            }
        }
    }

    public Collection<LakeSoulPartitionSplit> enumerateSplits() {
        List<PartitionInfo> allPartitionInfo = MetaVersion.getAllPartitionInfo(tableId);
        LOG.info("allPartitionInfo={}", allPartitionInfo);
        List<PartitionInfo> filteredPartition = SubstraitUtil.applyPartitionFilters(allPartitionInfo, partitionArrowSchema, partitionFilters);
        LOG.info("filteredPartition={}, filter={}", filteredPartition, partitionFilters);


        ArrayList<LakeSoulPartitionSplit> splits = new ArrayList<>(16);
        for (PartitionInfo partitionInfo : filteredPartition) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            long latestTimestamp = partitionInfo.getTimestamp() + 1;
            this.nextStartTime = Math.max(latestTimestamp, this.nextStartTime);

            DataFileInfo[] dataFileInfos;
            if (partitionLatestTimestamp.containsKey(partitionDesc)) {
                Long lastTimestamp = partitionLatestTimestamp.get(partitionDesc);
                LOG.info("getIncrementalPartitionDataInfo, startTime={}, endTime={}", lastTimestamp, latestTimestamp);
                dataFileInfos =
                        DataOperation.getIncrementalPartitionDataInfo(tableId, partitionDesc, lastTimestamp, latestTimestamp, "incremental");
            } else {
                dataFileInfos =
                        DataOperation.getIncrementalPartitionDataInfo(tableId, partitionDesc, startTime, latestTimestamp, "incremental");
            }
            if (dataFileInfos.length > 0) {
                Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                        FlinkUtil.splitDataInfosToRangeAndHashPartition(tableInfo, dataFileInfos);
                for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                    for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                        splits.add(new LakeSoulPartitionSplit(String.valueOf(split.hashCode()), split.getValue(), 0, split.getKey(), partitionDesc));
                    }
                }
            }
            partitionLatestTimestamp.put(partitionDesc, latestTimestamp);
        }
        LOG.info("partitionLatestTimestamp={}", partitionLatestTimestamp);

        return splits;
    }
}
