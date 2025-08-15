// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import org.apache.flink.lakesoul.entry.clean.PartitionInfoRecordGets.PartitionInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TickTriggeringCleaner extends RichCoFlatMapFunction<String, PartitionInfo, PartitionInfo> {

    private transient CleanUtils cleanUtils;
    private String pgUrl;
    private String userName;
    private String passWord;
    private long expiredTime;

    public TickTriggeringCleaner(String pgUrl, String userName, String passWord, long expiredTime) {
        this.pgUrl = pgUrl;
        this.userName = userName;
        this.passWord = passWord;
        this.expiredTime = expiredTime;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        cleanUtils = new CleanUtils();
    }

    @Override
    public void flatMap1(String tick, Collector<PartitionInfo> out) {
        try (Connection connection = DriverManager.getConnection(pgUrl, userName, passWord)) {
            cleanUtils.cleanDiscardFile(expiredTime, connection);
        } catch (SQLException e) {
            System.err.println("Failed to connect to database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void flatMap2(PartitionInfo value, Collector<PartitionInfo> out) {
        out.collect(value);
    }
}
