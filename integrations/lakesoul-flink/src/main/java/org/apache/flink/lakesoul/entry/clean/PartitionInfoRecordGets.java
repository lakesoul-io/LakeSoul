// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.dmetasoul.lakesoul.meta.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.api.common.functions.MapFunction;

import java.util.ArrayList;
import java.util.List;

public class PartitionInfoRecordGets {

    public static class metaMapper implements MapFunction<String, PartitionInfo> {
        List<String> tablesId;

        public metaMapper(List<String> tableNames) {
            this.tablesId = tableNames;
        }

        @Override
        public PartitionInfo map(String value) throws Exception {
            ObjectNode parse = JsonUtils.parseObject(value);
            String PGtableName = parse.path("tableName").asText();
            JsonNode commitJson = null;
            PartitionInfo partitionInfo = null;

            if (PGtableName.equals("partition_info")) {
                JsonNode after = parse.path("after");
                commitJson = after.isEmpty() ? parse.path("before") : after;
            }
            String eventOp = parse.path("commitOp").asText();
            if (!eventOp.equals("delete")) {
                String table_id = commitJson.path("table_id").asText(null);
                String partition_desc = commitJson.path("partition_desc").asText(null);
                String commit_op = commitJson.path("commit_op").asText(null);
                int version = commitJson.path("version").asInt();
                long timestamp = commitJson.path("timestamp").asLong();
                JsonNode snapshot = commitJson.path("snapshot");
                List<String> snapshotList = new ArrayList<>();
                for (JsonNode o : snapshot) {
                    snapshotList.add(o.asText());
                }
                if (tablesId == null) {
                    partitionInfo =
                            new PartitionInfo(
                                    table_id,
                                    partition_desc,
                                    version,
                                    commit_op,
                                    timestamp,
                                    snapshotList);
                } else {
                    for (String tableId : tablesId) {
                        if (tableId.equals(table_id)) {
                            partitionInfo =
                                    new PartitionInfo(
                                            table_id,
                                            partition_desc,
                                            version,
                                            commit_op,
                                            timestamp,
                                            snapshotList);
                            break;
                        }
                    }
                }

            } else {
                String table_id = commitJson.path("table_id").asText(null);
                String partition_desc = commitJson.path("partition_desc").asText(null);
                String commit_op = commitJson.path("commit_op").asText(null);
                int version = commitJson.path("version").asInt();
                long timestamp = -5L; // 标记这条数据被删除
                if (tablesId == null) {
                    partitionInfo =
                            new PartitionInfo(
                                    table_id, partition_desc, version, commit_op, timestamp, null);
                } else {
                    for (String tableId : tablesId) {
                        if (tableId.equals(table_id)) {
                            partitionInfo =
                                    new PartitionInfo(
                                            table_id,
                                            partition_desc,
                                            version,
                                            commit_op,
                                            timestamp,
                                            null);
                            break;
                        }
                    }
                }
            }
            return partitionInfo;
        }
    }

    public static class PartitionInfo {

        String tableId;
        String partitionDesc;
        int version;
        String commitOp;
        long timestamp;
        List<String> snapshot;

        public PartitionInfo(
                String tableId,
                String partitionDesc,
                int version,
                String commitOp,
                long timestamp,
                List<String> snapshot) {
            this.tableId = tableId;
            this.partitionDesc = partitionDesc;
            this.version = version;
            this.commitOp = commitOp;
            this.timestamp = timestamp;
            this.snapshot = snapshot;
        }

        @Override
        public String toString() {
            return "PartitionInfo{"
                    + "table_id='"
                    + tableId
                    + '\''
                    + ", partition_dec='"
                    + partitionDesc
                    + '\''
                    + ", version="
                    + version
                    + ", commit_op='"
                    + commitOp
                    + '\''
                    + ", timestamp="
                    + timestamp
                    + ", snapshot="
                    + snapshot
                    + '}';
        }

        public static class WillStateValue {
            Long timestamp;
            List<String> snapshot;

            public WillStateValue(Long timestamp, List<String> snapshot) {
                this.timestamp = timestamp;
                this.snapshot = snapshot;
            }
        }
    }
}
