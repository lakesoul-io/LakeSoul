// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.assets;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Desalting extends ProcessFunction<TableCounts, TableCounts> {
    private static final Logger log = LoggerFactory.getLogger(Desalting.class);

    @Override
    public void processElement(TableCounts value, ProcessFunction<TableCounts, TableCounts>.Context ctx, Collector<TableCounts> out) throws Exception {
        String tableId = value.getTableId();
        value.setTableId(tableId.split("\\$")[0]);
        log.info("=================================");
        out.collect(value);
    }
}
