// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import io.substrait.proto.Plan;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC;

public class LakeSoulAllPartitionDynamicSplitEnumerator
        implements SplitEnumerator<LakeSoulPartitionSplit, LakeSoulPendingSplits> {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulAllPartitionDynamicSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulPartitionSplit> context;

    private final LakeSoulDynSplitAssigner splitAssigner;
    private final long discoveryInterval;
    private final Map<String, Long> partitionLatestTimestamp;
    private final Set<Integer> taskIdsAwaitingSplit;
    private final Plan partitionFilters;
    private final List<String> partitionColumns;
    private final TableInfo tableInfo;
    protected Schema partitionArrowSchema;
    String tableId;
    String fullTableName;
    private long startTime;
    private long nextStartTime;
    private int hashBucketNum = -1;

    public LakeSoulAllPartitionDynamicSplitEnumerator(SplitEnumeratorContext<LakeSoulPartitionSplit> context,
                                                      LakeSoulDynSplitAssigner splitAssigner, RowType rowType,
                                                      long discoveryInterval, long startTime, String tableId,
                                                      String hashBucketNum, List<String> partitionColumns,
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
        List<Field>
                partitionFields =
                partitionColumns.stream().map(tableSchema::findField).collect(Collectors.toList());

        this.partitionArrowSchema = new Schema(partitionFields);
        this.partitionFilters = partitionFilters;
        tableInfo = DataOperation.dbManager().getTableInfoByTableId(tableId);
        fullTableName = tableInfo.getTableNamespace() + "." + tableInfo.getTableName();
        LOG.info("Create Dyn enumerator for table name {}, tableId {}, context {}",
                fullTableName, tableId, System.identityHashCode(context));
    }

    @Override
    public void start() {
        context.callAsync(this::enumerateSplits, this::processDiscoveredSplits, discoveryInterval, discoveryInterval);
    }

    @Override
    public synchronized void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        LOG.info("handleSplitRequest for {}, subTaskId {}, oid {}, tid {}",
                fullTableName, subtaskId, System.identityHashCode(this), Thread.currentThread().getId());
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }
        int tasksSize = context.registeredReaders().size();
        if (tasksSize == 0) {
            LOG.info("handleSplitRequest: Task size is 0 for subtaskId {} for table {}", subtaskId, fullTableName);
            taskIdsAwaitingSplit.add(subtaskId);
            return;
        }
        Optional<LakeSoulPartitionSplit> nextSplit = this.splitAssigner.getNext(subtaskId, tasksSize);
        if (nextSplit.isPresent()) {
            context.assignSplit(nextSplit.get(), subtaskId);
            taskIdsAwaitingSplit.remove(subtaskId);
        } else {
            taskIdsAwaitingSplit.add(subtaskId);
        }
    }

    @Override
    public synchronized void addSplitsBack(List<LakeSoulPartitionSplit> splits, int subtaskId) {
        LOG.info("Add split back: {} for {}, subTaskId {}, oid {}, tid {}",
                splits, fullTableName, subtaskId,
                System.identityHashCode(this),
                Thread.currentThread().getId());
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        List<LakeSoulPartitionSplit> remaining;
        synchronized (this) {
            remaining = splitAssigner.remainingSplits();
        }
        LakeSoulPendingSplits pendingSplits = new LakeSoulPendingSplits(
                remaining, this.nextStartTime, this.tableId,
                "", this.discoveryInterval, this.hashBucketNum);
        LOG.info("LakeSoulAllPartitionDynamicSplitEnumerator snapshotState, table {}, chkId {}, splits {}, oid {}, tid {}",
                fullTableName, checkpointId, pendingSplits,
                System.identityHashCode(this),
                Thread.currentThread().getId());
        return pendingSplits;
    }

    @Override
    public void close() throws IOException {

    }

    private synchronized void processDiscoveredSplits(
            Collection<LakeSoulPartitionSplit> splits, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate files for table {}", fullTableName, error);
            return;
        }
        int tasksSize = context.registeredReaders().size();
        LOG.info("Process discovered splits for table {}, {}, taskSize {}, oid {}, tid {}", splits,
                fullTableName, tasksSize, System.identityHashCode(this),
                Thread.currentThread().getId());
        this.splitAssigner.addSplits(splits);
        if (tasksSize == 0) {
            return;
        }
        Iterator<Integer> iter = taskIdsAwaitingSplit.iterator();
        while (iter.hasNext()) {
            int taskId = iter.next();
            if (!context.registeredReaders().containsKey(taskId)) {
                iter.remove();
                continue;
            }
            Optional<LakeSoulPartitionSplit> al = this.splitAssigner.getNext(taskId, tasksSize);
            if (al.isPresent()) {
                context.assignSplit(al.get(), taskId);
                iter.remove();
            }
        }
        LOG.info("Process discovered splits done for table {}, {}, oid {}, tid {}",
                fullTableName, splits,
                System.identityHashCode(this),
                Thread.currentThread().getId());
    }

    public Collection<LakeSoulPartitionSplit> enumerateSplits() {
        LOG.info("enumerateSplits begin for table {}, partition columns {}, oid {}, tid {}",
                fullTableName, partitionColumns,
                System.identityHashCode(this),
                Thread.currentThread().getId());
        long s = System.currentTimeMillis();
        List<PartitionInfo> allPartitionInfo;
        if (partitionColumns.isEmpty()) {
            allPartitionInfo = DataOperation.dbManager().getPartitionInfos(tableId,
                    Collections.singletonList(LAKESOUL_NON_PARTITION_TABLE_PART_DESC));
        } else {
            allPartitionInfo = MetaVersion.getAllPartitionInfo(tableId);
        }
        long e = System.currentTimeMillis();
        LOG.info("Table {} allPartitionInfo={}, queryTime={}ms", fullTableName, allPartitionInfo, e - s);
        List<PartitionInfo> filteredPartition = SubstraitUtil.applyPartitionFilters(
                allPartitionInfo, partitionArrowSchema, partitionFilters);
        LOG.info("Table {} filteredPartition={}, filter={}", fullTableName, filteredPartition, partitionFilters);

        ArrayList<LakeSoulPartitionSplit> splits = new ArrayList<>(16);
        for (PartitionInfo partitionInfo : filteredPartition) {
            String partitionDesc = partitionInfo.getPartitionDesc();
            long latestTimestamp = partitionInfo.getTimestamp() + 1;
            this.nextStartTime = Math.max(latestTimestamp, this.nextStartTime);

            Long lastTimestamp;
            synchronized (this) {
                lastTimestamp = partitionLatestTimestamp.get(partitionDesc);
            }
            DataFileInfo[] dataFileInfos;
            if (lastTimestamp != null) {
                LOG.info("getIncrementalPartitionDataInfo {}/{}, startTime={}, endTime={}",
                        fullTableName, partitionDesc,
                        lastTimestamp, latestTimestamp);
                if (lastTimestamp == latestTimestamp) {
                    // no timestamp change for this partition
                    LOG.info("Ignore partition for no new data {}/{}", fullTableName, partitionDesc);
                    continue;
                }
                dataFileInfos = DataOperation.getIncrementalPartitionDataInfo(
                        tableId, partitionDesc, lastTimestamp, latestTimestamp, "incremental");
            } else {
                LOG.info("new getIncrementalPartitionDataInfo {}/{}, startTime={}, endTime={}",
                        fullTableName, partitionDesc,
                        startTime, latestTimestamp);
                dataFileInfos = DataOperation.getIncrementalPartitionDataInfo(
                        tableId, partitionDesc, startTime, latestTimestamp, "incremental");
            }
            if (dataFileInfos.length > 0) {
                Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition =
                        FlinkUtil.splitDataInfosToRangeAndHashPartition(tableInfo, dataFileInfos);
                for (Map.Entry<String, Map<Integer, List<Path>>> entry : splitByRangeAndHashPartition.entrySet()) {
                    for (Map.Entry<Integer, List<Path>> split : entry.getValue().entrySet()) {
                        splits.add(new LakeSoulPartitionSplit(String.valueOf(split.hashCode()), split.getValue(),
                                0, split.getKey(), partitionDesc));
                    }
                }
            }
            synchronized (this) {
                partitionLatestTimestamp.put(partitionDesc, latestTimestamp);
            }
        }
        LOG.info("dynamic enumerate done, partitionLatestTimestamp={}, oid {}, tid {}",
                partitionLatestTimestamp,
                System.identityHashCode(this),
                Thread.currentThread().getId());

        return splits;
    }
}
