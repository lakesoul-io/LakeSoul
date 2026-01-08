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

            } else {
                String table_id = commitJson.getString("table_id");
                String partition_desc = commitJson.getString("partition_desc");
                String commit_op = commitJson.getString("commit_op");
                int version = commitJson.getInteger("version");
                long timestamp = -5L;//标记这条数据被删除
                if (tablesId == null){
                    partitionInfo = new PartitionInfo(table_id,partition_desc,version,commit_op,timestamp, null);
                }else {
                    for (String tableId : tablesId) {
                        if (tableId.equals(table_id)){
                            partitionInfo = new PartitionInfo(table_id,partition_desc,version,commit_op,timestamp, null);
                            break;
                        }
                    }
                }
            }
            return partitionInfo;
        }
    }

    public static class PartitionInfo{

        String tableId;
        String partitionDesc;
        int version;
        String commitOp;
        long timestamp;
        List<String> snapshot;

        public PartitionInfo(String tableId, String partitionDesc, int version, String commitOp, long timestamp, List<String> snapshot) {
            this.tableId = tableId;
            this.partitionDesc = partitionDesc;
            this.version = version;
            this.commitOp = commitOp;
            this.timestamp = timestamp;
            this.snapshot = snapshot;
        }

        @Override
        public String toString() {
            return "PartitionInfo{" +
                    "table_id='" + tableId + '\'' +
                    ", partition_dec='" + partitionDesc + '\'' +
                    ", version=" + version +
                    ", commit_op='" + commitOp + '\'' +
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
