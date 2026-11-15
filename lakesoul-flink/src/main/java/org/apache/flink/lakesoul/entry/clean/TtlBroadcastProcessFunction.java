// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DataFileReadLeaseGuard;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;

import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TtlBroadcastProcessFunction
        extends KeyedBroadcastProcessFunction<
                String,
                TableTtlProFunction.PartitionINfoUpdateEvents,
                TableInfoRecordGets.TableInfo,
                String> {

    private static final Logger log = LoggerFactory.getLogger(TtlBroadcastProcessFunction.class);
    private final long maxProcessIntervalMillis;

    private final MapStateDescriptor<String, Integer> ttlBroadcastStateDesc;
    private transient ValueState<Long> partitionLatestFreshTimeState;
    private transient ValueState<Long> partitionLatestProcessTimeState;
    private transient ValueState<Long> partitionTimerTimestampState;

    public TtlBroadcastProcessFunction(
            MapStateDescriptor<String, Integer> ttlBroadcastStateDesc, int maxProcessIntervalDays) {

        this.ttlBroadcastStateDesc = ttlBroadcastStateDesc;
        this.maxProcessIntervalMillis = TimeUnit.DAYS.toMillis(maxProcessIntervalDays);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> partitionLatestFreshTimeStateDesc =
                new ValueStateDescriptor<>("partitionLatestFreshTimeState", Long.class);
        partitionTimerTimestampState =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("timerTsState", Long.class));

        ValueStateDescriptor<Long> partitionLatestProcessTimeStateDesc =
                new ValueStateDescriptor<>("partitionLatestProcessTimeState", Long.class);
        partitionLatestFreshTimeState =
                getRuntimeContext().getState(partitionLatestFreshTimeStateDesc);
        partitionLatestProcessTimeState =
                getRuntimeContext().getState(partitionLatestProcessTimeStateDesc);
    }

    @Override
    public void processElement(
            TableTtlProFunction.PartitionINfoUpdateEvents value,
            KeyedBroadcastProcessFunction<
                                    String,
                                    TableTtlProFunction.PartitionINfoUpdateEvents,
                                    TableInfoRecordGets.TableInfo,
                                    String>
                            .ReadOnlyContext
                    ctx,
            Collector<String> out)
            throws Exception {
        Long updateTimestamp = value.timestamp;
        Long latestTimestamp = partitionLatestFreshTimeState.value();
        long currentProcTime = ctx.timerService().currentProcessingTime();
        if (latestTimestamp == null || updateTimestamp > latestTimestamp) {
            partitionLatestFreshTimeState.update(updateTimestamp);
            partitionLatestProcessTimeState.update(currentProcTime);
        }
        Long oldTimerTs = partitionTimerTimestampState.value();
        if (oldTimerTs != null) {
            ctx.timerService().deleteProcessingTimeTimer(oldTimerTs);
        }
        long nextCheckTime = partitionLatestProcessTimeState.value() + maxProcessIntervalMillis;
        ctx.timerService().registerProcessingTimeTimer(nextCheckTime);
        partitionTimerTimestampState.update(nextCheckTime);
    }

    @Override
    public void processBroadcastElement(
            TableInfoRecordGets.TableInfo value,
            KeyedBroadcastProcessFunction<
                                    String,
                                    TableTtlProFunction.PartitionINfoUpdateEvents,
                                    TableInfoRecordGets.TableInfo,
                                    String>
                            .Context
                    ctx,
            Collector<String> out)
            throws Exception {
        String tableId = value.tableId;
        int partitionTtl = value.partitionTtl;
        BroadcastState<String, Integer> broadcastState =
                ctx.getBroadcastState(ttlBroadcastStateDesc);
        if (partitionTtl == -1) {
            log.info("检测到用户取消表：{} partition.ttl配置 ，清理相关状态", tableId);
            broadcastState.remove(tableId);
        } else if (partitionTtl == -5) {
            log.info("检测到表：{} 已经被删除，清理相关状态", tableId);
            broadcastState.remove(tableId);
            partitionTimerTimestampState.clear();
            partitionLatestFreshTimeState.clear();
            partitionLatestProcessTimeState.clear();
        } else {
            broadcastState.put(tableId, partitionTtl);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        long currentProcTime = timestamp;
        Long timerTs = partitionTimerTimestampState.value();
        if (timerTs == null || timerTs != timestamp) {
            return;
        }
        String partitionKey = ctx.getCurrentKey();
        String[] split = partitionKey.split("/");
        String tableId = split[0];
        String partitionDesc = split[1];
        ReadOnlyBroadcastState<String, Integer> broadcastState =
                ctx.getBroadcastState(ttlBroadcastStateDesc);
        if (broadcastState == null) {
            return;
        }
        if (broadcastState.contains(tableId)) {
            int partitionTtl = broadcastState.get(tableId);
            if (partitionTtl == -1) {
                partitionLatestProcessTimeState.clear();
            } else {
                long expiredTime = TimeUnit.DAYS.toMillis(partitionTtl);
                Long latestFreshTime = partitionLatestFreshTimeState.value();
                Long lastProcessTime = partitionLatestProcessTimeState.value();
                if (latestFreshTime == null || lastProcessTime == null) {
                    return;
                }
                long sinceLastUpdate = currentProcTime - latestFreshTime;
                long sinceLastProcess = currentProcTime - lastProcessTime;
                boolean updateExpired = sinceLastUpdate >= expiredTime;
                boolean processTooOld = sinceLastProcess >= maxProcessIntervalMillis;

                if (updateExpired && processTooOld) {
                    if (dropPartition(tableId, partitionDesc)) {
                        partitionLatestProcessTimeState.clear();
                        partitionLatestFreshTimeState.clear();
                        partitionTimerTimestampState.clear();
                    } else {
                        long nextCheckTime = currentProcTime + maxProcessIntervalMillis;
                        ctx.timerService().registerProcessingTimeTimer(nextCheckTime);
                        partitionTimerTimestampState.update(nextCheckTime);
                    }
                } else {
                    long nextCheckTime = currentProcTime + maxProcessIntervalMillis;
                    log.info("table_id: {},分区 {}并没有过期，将在后续继续判断", tableId, partitionDesc);
                    ctx.timerService().registerProcessingTimeTimer(nextCheckTime);
                    partitionTimerTimestampState.update(nextCheckTime);
                }
            }
        } else {
            long nextCheckTime = currentProcTime + maxProcessIntervalMillis;
            ctx.timerService().registerProcessingTimeTimer(nextCheckTime);
            partitionTimerTimestampState.update(nextCheckTime);
        }
    }

    public boolean dropPartition(String tableId, String partitionDesc) throws CatalogException {
        DBManager dbManager = new DBManager();
        TableInfo tableInfo = dbManager.getTableInfoByTableId(tableId);
        if (tableInfo == null) {
            throw new CatalogException("Table " + tableId + " does not exist");
        }
        try (Connection connection = DBConnector.getConn();
                DataFileReadLeaseGuard readers = DataFileReadLeaseGuard.acquire(connection, tableId)) {
            if (readers.hasActiveReaders()) {
                return false;
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "select 1 from partition_info where table_id = ? and partition_desc = ? and pinned = true limit 1")) {
                statement.setString(1, tableId);
                statement.setString(2, partitionDesc);
                try (ResultSet rows = statement.executeQuery()) {
                    if (rows.next()) {
                        return false;
                    }
                }
            }
            List<String> paths = new ArrayList<>();
            try (PreparedStatement statement = connection.prepareStatement(
                    "select distinct f.path from partition_info p join data_commit_info d"
                            + " on d.table_id = p.table_id and d.partition_desc = p.partition_desc"
                            + " and d.commit_id = any(p.snapshot), unnest(d.file_ops) f"
                            + " where p.table_id = ? and p.partition_desc = ?")) {
                statement.setString(1, tableId);
                statement.setString(2, partitionDesc);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        paths.add(rows.getString(1));
                    }
                }
            }
            CleanUtils cleanUtils = new CleanUtils();
            Path partitionDir = null;
            for (String filePath : paths) {
                if (!readers.retire(filePath, false)) {
                    return false;
                }
                cleanUtils.deleteFile(filePath);
                if (partitionDir == null) {
                    partitionDir = new Path(filePath).getParent();
                }
            }
            // Metadata remains available after any failed deletion. Native
            // acquisition rejects the durable file fences until a fresh
            // partition snapshot no longer references the retired paths.
            dbManager.deleteMetaPartitionInfo(tableInfo.getTableId(), partitionDesc);
            if (partitionDir != null) {
                FileSystem fs = partitionDir.getFileSystem();
                if (fs.exists(partitionDir) && fs.listStatus(partitionDir).length == 0) {
                    // Partition roots are reusable: never recursively remove
                    // a concurrent writer's new files or fence the directory.
                    fs.delete(partitionDir, false);
                }
            }
            readers.commit();
            return true;
        } catch (SQLException | IOException error) {
            throw new CatalogException("failed to clean partition " + partitionDesc, error);
        }
    }
}
