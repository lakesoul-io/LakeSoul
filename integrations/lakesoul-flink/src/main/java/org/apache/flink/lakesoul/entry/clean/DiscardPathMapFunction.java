// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.dmetasoul.lakesoul.meta.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * Maps a discard_compressed_file_info CDC record to (file path, file timestamp, retention base).
 *
 * <p>The retention base is the time the file was marked as discarded, i.e. the CDC event time for
 * streaming records. Using the file's modification time directly deletes files that were written
 * days ago immediately after a compaction, which races with readers that resolved the previous
 * snapshot. Snapshot records (the initial replay of the backlog) keep the file timestamp so that
 * previously accumulated discard rows are not delayed by another retention period.
 */
public class DiscardPathMapFunction implements MapFunction<String, Tuple3<String, Long, Long>> {

    @Override
    public Tuple3<String, Long, Long> map(String value) throws Exception {

        ObjectNode parse = JsonUtils.parseObject(value);
        String PGtableName = parse.path("tableName").asText();
        JsonNode commitJson = null;
        Tuple3<String, Long, Long> filePathInfo = new Tuple3<>();
        if (PGtableName.equals("discard_compressed_file_info")) {
            JsonNode after = parse.path("after");
            commitJson = after.isEmpty() ? parse.path("before") : after;
        }
        String eventOp = parse.path("commitOp").asText();
        if (!eventOp.equals("delete")) {
            String filePath = commitJson.path("file_path").asText();
            long fileTimestamp = commitJson.path("timestamp").asLong();
            long eventTime = parse.path("eventTime").asLong();
            boolean fromSnapshot = parse.path("snapshot").asBoolean();
            long retentionBase = fromSnapshot || eventTime <= 0 ? fileTimestamp : eventTime;
            filePathInfo.f0 = filePath;
            filePathInfo.f1 = fileTimestamp;
            filePathInfo.f2 = retentionBase;
            return filePathInfo;
        } else {
            filePathInfo.f0 = commitJson.path("file_path").asText();
            filePathInfo.f1 = -5L;
            filePathInfo.f2 = -5L;
            return filePathInfo;
        }
    }
}
