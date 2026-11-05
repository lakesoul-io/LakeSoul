// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DiscardFilePathProcessFunction
        extends KeyedProcessFunction<String, Tuple3<String, Long, Long>, String> {

    private static final Logger log = LoggerFactory.getLogger(DiscardFilePathProcessFunction.class);
    private final long expiredTimestamp;

    private transient ValueState<Long> lastOntimerTimestampState;
    private transient ValueState<Long> recordTimestampState;

    public DiscardFilePathProcessFunction(long expiredTimestamp) {
        this.expiredTimestamp = expiredTimestamp;
    }

    @Override
    public void open(Configuration parameters) {
        // 初始化状态
        ValueStateDescriptor<Long> recordTimestampStateDesc =
                new ValueStateDescriptor<>("recordTimestampState", Long.class);
        recordTimestampState = getRuntimeContext().getState(recordTimestampStateDesc);

        ValueStateDescriptor<Long> lastOntimerTimestampStateDesc =
                new ValueStateDescriptor<>("lastUpdateTimestampState", Long.class);
        lastOntimerTimestampState = getRuntimeContext().getState(lastOntimerTimestampStateDesc);
    }

    @Override
    public void processElement(Tuple3<String, Long, Long> value, Context ctx, Collector<String> out)
            throws Exception {

        String filePath = value.f0;
        long fileTimestamp = value.f1;
        // Retention starts when the file was marked as discarded (CDC event time), not when the
        // file was written; see DiscardPathMapFunction. Snapshot records keep the file timestamp.
        long retentionBase = value.f2;
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        if (fileTimestamp != -5L) {
            recordTimestampState.update(retentionBase);
            if (currentProcessingTime - retentionBase > expiredTimestamp) {
                log.info("文件 [{}] 已过期，加入清理队列准备清理。", filePath);
                out.collect(value.f0);
                recordTimestampState.clear();
            } else {
                long triggerTime = retentionBase + expiredTimestamp;
                lastOntimerTimestampState.update(triggerTime);
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
                LocalDateTime dateTime =
                        LocalDateTime.ofInstant(
                                Instant.ofEpochMilli(triggerTime), ZoneId.systemDefault());
                String formatted =
                        dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                log.info("文件 [{}] 未过期，注册定时器将在 {} 触发清理。", filePath, formatted);
            }
        } else {
            if (lastOntimerTimestampState.value() != null) {
                log.info("文件:" + filePath + "已在别处被清理，删除该文件相关的定时器和状态");
                ctx.timerService().deleteProcessingTimeTimer(lastOntimerTimestampState.value());
                recordTimestampState.clear();
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        String filePath = ctx.getCurrentKey();
        Long retentionBase = recordTimestampState.value();
        if (retentionBase == null) return;
        log.info("文件 [{}] 已过期，加入清理队列准备清理。", filePath);
        out.collect(filePath);
        recordTimestampState.clear();
        lastOntimerTimestampState.clear();
    }
}
