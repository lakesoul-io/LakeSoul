// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

public class TableInfoRecordGets {
    public static class tableInfoMapper implements MapFunction<String, TableInfo> {

        @Override
        public TableInfo map(String value) {
            JSONObject tableInfoParse = (JSONObject) JSONObject.parse(value);
            JSONObject properties = (JSONObject) tableInfoParse.get("after");
            String tableId = properties.getString("tableId");
            if (properties.containsKey("partition.ttl")){
                int partitionTtl = properties.getInteger("partition.ttl");
                return new TableInfo(partitionTtl, tableId);
            } else if (properties.containsKey("operation")){
                //which means table has been dropped;
               return new TableInfo(-5, tableId);
            } else {
                //which means user canceled the partition.ttl
                return new TableInfo(-1, tableId);
            }
        }
    }

    public static class TableInfo{
        String tableId;
        int partitionTtl;

        public TableInfo(int partitionTtl, String tableId) {
            this.partitionTtl = partitionTtl;
            this.tableId = tableId;
        }

        @Override
        public String toString() {
            return "TableInfo{" +
                    "tableId='" + tableId + '\'' +
                    ", partitionTtl=" + partitionTtl +
                    '}';
        }
    }

}
