// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.test;

import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.lakesoul.entry.clean.DiscardPathMapFunction;
import org.junit.Test;

public class DiscardPathMapFunctionTest {

    private static String record(
            String commitOp, JSONObject row, Long eventTime, boolean snapshot) {
        JSONObject json = new JSONObject();
        json.put("tableName", "discard_compressed_file_info");
        json.put("commitOp", commitOp);
        json.put("after", row);
        json.put("before", new JSONObject());
        if (eventTime != null) {
            json.put("eventTime", eventTime);
        }
        json.put("snapshot", snapshot);
        return json.toJSONString();
    }

    @Test
    public void streamingRecordStartsRetentionAtEventTime() throws Exception {
        JSONObject row = new JSONObject();
        row.put("file_path", "s3://bucket/file.parquet");
        row.put("timestamp", 1_000L);

        Tuple3<String, Long, Long> result =
                new DiscardPathMapFunction().map(record("insert", row, 9_000L, false));

        assertEquals("s3://bucket/file.parquet", result.f0);
        assertEquals(1_000L, result.f1.longValue());
        assertEquals(9_000L, result.f2.longValue());
    }

    @Test
    public void snapshotRecordKeepsFileTimestamp() throws Exception {
        JSONObject row = new JSONObject();
        row.put("file_path", "s3://bucket/file.parquet");
        row.put("timestamp", 1_000L);

        Tuple3<String, Long, Long> result =
                new DiscardPathMapFunction().map(record("insert", row, 9_000L, true));

        assertEquals(1_000L, result.f2.longValue());
    }

    @Test
    public void recordWithoutEventTimeFallsBackToFileTimestamp() throws Exception {
        JSONObject row = new JSONObject();
        row.put("file_path", "s3://bucket/file.parquet");
        row.put("timestamp", 1_000L);

        Tuple3<String, Long, Long> result =
                new DiscardPathMapFunction().map(record("insert", row, null, false));

        assertEquals(1_000L, result.f2.longValue());
    }

    @Test
    public void deleteRecordKeepsMinusFiveMarker() throws Exception {
        JSONObject before = new JSONObject();
        before.put("file_path", "s3://bucket/file.parquet");
        JSONObject json = new JSONObject();
        json.put("tableName", "discard_compressed_file_info");
        json.put("commitOp", "delete");
        json.put("after", new JSONObject());
        json.put("before", before);

        Tuple3<String, Long, Long> result = new DiscardPathMapFunction().map(json.toJSONString());

        assertEquals("s3://bucket/file.parquet", result.f0);
        assertEquals(-5L, result.f1.longValue());
    }
}
