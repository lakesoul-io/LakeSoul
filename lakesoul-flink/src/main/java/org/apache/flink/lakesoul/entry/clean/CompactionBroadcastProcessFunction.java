// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CompactionBroadcastProcessFunction extends KeyedBroadcastProcessFunction<
        String,
        PartitionInfoRecordGets.PartitionInfo,
        CompactProcessFunction.CompactionOut,
        PartitionInfoRecordGets.PartitionInfo> {
    private static final Logger log = LoggerFactory.getLogger(CompactionBroadcastProcessFunction.class);

    private final MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc;
    private transient ValueState<PartitionInfoRecordGets.PartitionInfo> elementState;
    private ValueState<Long> timerTsState;

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;
    private final int expiredTime;
    private final long ontimerInterval;
    private static CleanUtils cleanUtils;
    private transient DataSource dataSource;

    public CompactionBroadcastProcessFunction(MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc, String pgUrl, String pgUserName, String pgPasswd, int expiredTime, long ontimerInterval) {
        this.broadcastStateDesc = broadcastStateDesc;
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
        this.expiredTime = expiredTime;
        this.ontimerInterval = ontimerInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<PartitionInfoRecordGets.PartitionInfo> desc =
                new ValueStateDescriptor<>(
                        "elementState",
                        PartitionInfoRecordGets.PartitionInfo.class
                );
        timerTsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timerTsState", Long.class)
        );
        elementState = getRuntimeContext().getState(desc);
        cleanUtils = new CleanUtils();
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pgUrl);
        config.setUsername(pgUserName);
        config.setPassword(pgPasswd);
        config.setDriverClassName("org.postgresql.Driver");

        config.setMaximumPoolSize(5);            // 每个 TM 的最大连接数
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10000);      // 10 秒超时
        config.setIdleTimeout(60000);            // 1 分钟空闲回收
        config.setMaxLifetime(300000);           // 5 分钟重建连接
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
    }

    @Override
    public void processBroadcastElement(
            CompactProcessFunction.CompactionOut value,
            Context ctx,
            Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {

        BroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);

        String key = value.getTableId() + "/" + value.getPartitionDesc();
        CompactProcessFunction.CompactionOut current = state.get(key);

        if (current == null) {
            state.put(key, value);
        } else {
            // 计算最大的 switchVersion
            long maxSwitchVersion = Math.max(current.switchVersion, value.switchVersion);
            if (value.getTimestamp() > current.getTimestamp()) {
                // 如果新来的 timestamp 更大 → 用新值覆盖，但 switchVersion 保留最大
                state.put(key, new CompactProcessFunction.CompactionOut(
                        value.getTableId(),
                        value.getPartitionDesc(),
                        value.getVersion(),
                        value.getTimestamp(),
                        value.isOldCompaction(),
                        maxSwitchVersion
                ));
            } else {
                // timestamp 旧 → 保留 current，但更新 switchVersion 为最大
                if (maxSwitchVersion > current.switchVersion) {
                    state.put(key, new CompactProcessFunction.CompactionOut(
                            current.getTableId(),
                            current.getPartitionDesc(),
                            current.getVersion(),
                            current.getTimestamp(),
                            current.isOldCompaction(),
                            maxSwitchVersion
                    ));
                }
            }
        }
    }

    @Override
    public void processElement(
            PartitionInfoRecordGets.PartitionInfo value,
            ReadOnlyContext ctx,
            Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {
        ReadOnlyBroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);
        String key = value.tableId + "/" + value.partitionDesc;
        long valueTimestamp = value.timestamp;
        CompactProcessFunction.CompactionOut compaction = state.get(key);
        if (valueTimestamp == -5L) {
            if (elementState.value() != null) {
                log.info("检测到[{}] 在其他地方被清理，清理相关状态",ctx.getCurrentKey());
                //System.out.println("检测到" + ctx.getCurrentKey() + " 在其他地方被清理，清理相关状态");
                elementState.clear();
                if (timerTsState.value() != null) {
                    ctx.timerService().deleteProcessingTimeTimer(timerTsState.value());
                    //如果识别出大于最新compaction记录的数据被删除，识为为该分区被删除，清理相关状态
                    if (compaction != null){
                        if (value.version >= state.get(key).version){
                            log.info("清除" + state.get(key) +"压缩状态");
                            state.clear();
                        }
                    }
                }
            }
        } else {
            elementState.update(value);
            if (compaction != null) {
                // enrich 主流数据
                long compactTimstamp = compaction.timestamp;
                long currTimestamp = System.currentTimeMillis();
                if (valueTimestamp < compactTimstamp && currTimestamp - valueTimestamp > expiredTime){
                    log.info(ctx.getCurrentKey() + " 执行删除操作");
                    CleanUtils cleanUtils = new CleanUtils();
                    boolean latestCompactVersionIsOld = state.get(key).isOldCompaction();
                    boolean belongOldCompaction = value.version < state.get(key).switchVersion || latestCompactVersionIsOld;
                    try (Connection connection = dataSource.getConnection()) {
                        cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.tableId, value.partitionDesc, connection, belongOldCompaction);
                        cleanUtils.cleanPartitionInfo(value.tableId, value.partitionDesc, value.version, connection);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    log.info(ctx.getCurrentKey() + " 执行旧版清理： " + belongOldCompaction);
                    elementState.clear();
                } else {
                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                    long triggerTime = currentProcessingTime + ontimerInterval;
                    timerTsState.update(triggerTime);
                    LocalDateTime dateTime = LocalDateTime.ofInstant(
                            Instant.ofEpochMilli(triggerTime),
                            ZoneId.systemDefault());
                    String formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    log.info(ctx.getCurrentKey() + "注册定时器，将在" + formatted + " 执行");
                    ctx.timerService().registerProcessingTimeTimer(triggerTime);
                }
            } else {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                long triggerTime = currentProcessingTime + ontimerInterval;
                timerTsState.update(triggerTime);
                LocalDateTime dateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(triggerTime),
                        ZoneId.systemDefault());
                String formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                log.info(ctx.getCurrentKey() + "注册定时器，将在" + formatted + " 执行");
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {

        String currentKey = ctx.getCurrentKey();
        String[] split = currentKey.split("/");
        String tableId = split[0];
        String partitionDesc = split[1];
        ReadOnlyBroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);
        String partitionDescKey = tableId + "/" + partitionDesc;
        CompactProcessFunction.CompactionOut compactionOut = state.get(partitionDescKey);
        PartitionInfoRecordGets.PartitionInfo value = elementState.value();
        if (compactionOut != null) {
            log.info(currentKey + state.get(partitionDescKey).switchVersion);
            long eventTimestamp = value.timestamp;
            long compactTimestamp = compactionOut.timestamp;
            if (eventTimestamp < compactTimestamp && timestamp - eventTimestamp > expiredTime){
                boolean latestCompactVersionIsOld = state.get(partitionDescKey).isOldCompaction();
                boolean belongOldCompaction = value.version < state.get(partitionDescKey).switchVersion || latestCompactVersionIsOld;
                log.info(currentKey + " 执行旧版清理： " + belongOldCompaction);
                try (Connection connection = dataSource.getConnection()) {
                    cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.tableId, value.partitionDesc, connection, belongOldCompaction);
                    cleanUtils.cleanPartitionInfo(value.tableId, value.partitionDesc, value.version, connection);
                    timerTsState.clear();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                elementState.clear();
            } else {
                long triggerTime = timestamp + ontimerInterval;
                timerTsState.update(triggerTime);
                LocalDateTime dateTime = LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(triggerTime),
                        ZoneId.systemDefault());
                String formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                log.info(currentKey + "注册定时器，将在" + formatted + " 执行");
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }
        } else {
            long triggerTime = timestamp + ontimerInterval;
            timerTsState.update(triggerTime);
            LocalDateTime dateTime = LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(triggerTime),
                    ZoneId.systemDefault());
            String formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            log.info(currentKey + "注册定时器，将在" + formatted + " 执行");
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
        }
    }
}