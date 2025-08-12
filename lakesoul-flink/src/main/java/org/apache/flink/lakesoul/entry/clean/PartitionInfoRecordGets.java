// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import java.util.ArrayList;
import java.util.List;

public class PartitionInfoRecordGets {

    public static class metaMapper implements MapFunction<String, PartitionInfo>{
        List<String> tablesId;

        public metaMapper(List<String> tableNames) {
            this.tablesId = tableNames;
        }

        @Override
        public PartitionInfo map(String value) throws Exception {
            JSONObject parse = (JSONObject) JSONObject.parse(value);
            String PGtableName = parse.get("tableName").toString();
            JSONObject commitJson = null;
            PartitionInfo partitionInfo = null;

            if (PGtableName.equals("partition_info")){
                if (!parse.getJSONObject("after").isEmpty()){
                    commitJson = (JSONObject) parse.get("after");
                } else {
                    commitJson = (JSONObject) parse.get("before");
                }
            }
            String eventOp = parse.getString("commitOp");
            if (!eventOp.equals("delete")){
                String table_id = commitJson.getString("table_id");
                String partition_desc = commitJson.getString("partition_desc");
                String commit_op = commitJson.getString("commit_op");
                int version = commitJson.getInteger("version");
                long timestamp = commitJson.getLong("timestamp");
                JSONArray snapshot = commitJson.getJSONArray("snapshot");
                List<String> snapshotList = new ArrayList<>();
                for (Object o : snapshot) {
                    snapshotList.add(o.toString());
                }
                if (tablesId == null){
                    partitionInfo = new PartitionInfo(table_id,partition_desc,version,commit_op,timestamp, snapshotList);
                }else {
                    for (String tableId : tablesId) {
                        if (tableId.equals(table_id)){
                            partitionInfo = new PartitionInfo(table_id,partition_desc,version,commit_op,timestamp, snapshotList);
                            break;
                        }
                    }
                }

            }
            return partitionInfo;
        }
    }

    public static class PartitionInfo{

        String table_id;
        String partition_desc;
        int version;
        String commit_op;
        long timestamp;
        List<String> snapshot;

        public PartitionInfo(String table_id, String partition_dec, int version, String commit_op, long timestamp, List<String> snapshot) {
            this.table_id = table_id;
            this.partition_desc = partition_dec;
            this.version = version;
            this.commit_op = commit_op;
            this.timestamp = timestamp;
            this.snapshot = snapshot;
        }

        public String getTable_id() {
            return table_id;
        }

        public void setTable_id(String table_id) {
            this.table_id = table_id;
        }

        public String getPartition_desc() {
            return partition_desc;
        }

        public void setPartition_desc(String partition_desc) {
            this.partition_desc = partition_desc;
        }

        public int getVersion() {
            return version;
        }

        public void setVersion(int version) {
            this.version = version;
        }

        public String getCommit_op() {
            return commit_op;
        }

        public void setCommit_op(String commit_op) {
            this.commit_op = commit_op;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public List<String> getSnapshot() {
            return snapshot;
        }

        public void setSnapshot(List<String> snapshot) {
            this.snapshot = snapshot;
        }

        @Override
        public String toString() {
            return "PartitionInfo{" +
                    "table_id='" + table_id + '\'' +
                    ", partition_dec='" + partition_desc + '\'' +
                    ", version=" + version +
                    ", commit_op='" + commit_op + '\'' +
                    ", timestamp=" + timestamp +
                    ", snapshot=" + snapshot +
                    '}';
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

