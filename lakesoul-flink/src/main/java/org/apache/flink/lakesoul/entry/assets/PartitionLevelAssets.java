package org.apache.flink.lakesoul.entry.assets;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class PartitionLevelAssets {
    private static final Logger log = LoggerFactory.getLogger(PartitionLevelAssets.class);

    public static class metaMapper implements MapFunction<String, Tuple3<String, String, String[]>> {

        @Override
        public Tuple3<String, String, String[]> map(String s) {
            // 解析传入的 JSON 字符串
            JSONObject parse = (JSONObject) JSONObject.parse(s);
            String PGtableName = parse.get("tableName").toString();
            String[] tableInfos = new String[6];
            JSONObject commitJson;

            // 处理 data_commit_info 表
            if (PGtableName.equals("data_commit_info")) {
                String beforeCommitted = "true";
                if (parse.getJSONObject("before").size()>0){
                    JSONObject before =(JSONObject) parse.get("before");
                    beforeCommitted = before.getString("committed");
                }
                if (parse.getJSONObject("after").size() > 0){
                    commitJson = (JSONObject) parse.get("after");
                } else {
                    commitJson = (JSONObject) parse.get("before");
                }

                String tableId = commitJson.getString("table_id");
                JSONArray fileOps = commitJson.getJSONArray("file_ops");
                String committed = commitJson.getString("committed");
                String commitOp = commitJson.getString("commit_op");
                String eventOp = parse.getString("commitOp");
                String partitionDesc = commitJson.getString("partition_desc");
                long fileBytesSize = 0L;
                int fileCount = 0;
                for (Object op : fileOps) {
                    byte[] decode = Base64.getDecoder().decode(op.toString());
                    String fileOp = new String(decode, StandardCharsets.UTF_8);
                    AssetsUtils assetsUtils = new AssetsUtils();
                    String[] fileOpsString = assetsUtils.parseFileOpsString(fileOp);
                    fileCount ++;
                    fileBytesSize = fileBytesSize + Long.parseLong(fileOpsString[1]);
                }

                tableInfos[0] = String.valueOf(fileCount);
                tableInfos[1] = String.valueOf(fileBytesSize);
                tableInfos[2] = committed;
                tableInfos[3] = eventOp;
                tableInfos[4] = commitOp;
                tableInfos[5] = beforeCommitted;
                boolean dealingWithSkew = CountDataAssets.dealingDataSkew;
                if (dealingWithSkew){
                    log.info("开启数据倾斜处理");
                    Random random = new Random();
                    int randomNumer = random.nextInt(2);
                    return new Tuple3<>(PGtableName, tableId+ "$" + randomNumer+" "+partitionDesc, tableInfos);
                } else {
                    return new Tuple3<>(PGtableName, tableId + " "+partitionDesc, tableInfos);
                }

            }
            return null;
        }
    }

//    public static class sideProcessing extends ProcessFunction<Tuple3<String, String, String[]>, Tuple3<String, String, String[]>> {
//        @Override
//        public void processElement(Tuple3<String, String, String[]> value, Context ctx, Collector<Tuple3<String, String, String[]>> out) throws Exception {
//            // 根据表名选择旁数据流
//            switch (value.f0) {
//                case "table_info":
//                    ctx.output(new OutputTag<Tuple3<String, String, String[]>>("table_info") {
//                    }, value);
//                    break;
//                case "data_commit_info":
//                    ctx.output(new OutputTag<Tuple3<String, String, String[]>>("data_commit_info") {
//                    }, value);
//                    break;
//            }
//        }
//    }
    public static class PartitionLevelProcessFunction extends ProcessFunction<Tuple3<String, String, String[]>, PartitionCounts> {

        // 定义一个 ValueState 来存储累加的值
        private transient ValueState<Integer> partitionBaseFileCountValue;
        private transient ValueState<Integer> partitionTotalFileCountValue;
        private transient ValueState<Long> partitionBaseSizeValue;
        private transient ValueState<Long> partitionTotalSizeValue;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化累加状态
            ValueStateDescriptor<Integer> partitionBaseFileCountDescriptor = new ValueStateDescriptor<>(
                    "partitionBaseFileCountValue", // 状态的名称
                    Integer.class,      // 状态的类型
                    0                   // 默认值为 0
            );
            partitionBaseFileCountValue = getRuntimeContext().getState(partitionBaseFileCountDescriptor);
            ValueStateDescriptor<Integer> partitionTotalFileCountDescriptor = new ValueStateDescriptor<>(
                    "partitionTotalFileCountValue", // 状态的名称
                    Integer.class,      // 状态的类型
                    0                   // 默认值为 0
            );
            partitionTotalFileCountValue = getRuntimeContext().getState(partitionTotalFileCountDescriptor);

            ValueStateDescriptor<Long> partitionBaseSizeDescriptor = new ValueStateDescriptor<>(
                    "partitionBaseSizeValue", // 状态的名称
                    Long.class,          // 状态的类型
                    0L                   // 默认值为 0L
            );
            partitionBaseSizeValue = getRuntimeContext().getState(partitionBaseSizeDescriptor);

            ValueStateDescriptor<Long> partitionTotalSizeValueDescriptor = new ValueStateDescriptor<>(
                    "partitionTotalSizeValue", // 状态的名称
                    Long.class,          // 状态的类型
                    0L                   // 默认值为 0L
            );
            partitionTotalSizeValue = getRuntimeContext().getState(partitionTotalSizeValueDescriptor);

        }

        @Override
        public void processElement(Tuple3<String, String, String[]> value, Context ctx, Collector<PartitionCounts> out) throws Exception {
            //PartitionCounts partitionCounts = new PartitionCounts()
            //System.out.println(value);
            String tableId = value.f1.split(" ")[0];
            String partitionDesc = value.f1.split(" ")[1];
            int currentFileCountValue = Integer.parseInt(value.f2[0]);
            long currentFileBytesSize = Long.parseLong(value.f2[1]);
            boolean committed = Boolean.parseBoolean(value.f2[2]);
            boolean beforeCommitted = Boolean.parseBoolean(value.f2[5]);
            String commitOp = value.f2[4];
            String enventOp = value.f2[3];
            // 获取当前状态中的累加值
            int previousTotalFileCountValue = partitionTotalFileCountValue.value();
            int previousBaseFileCountValue = partitionBaseFileCountValue.value();
            long fileBytesSizePreviousTotalValue = partitionTotalSizeValue.value();
            long fileBytesSizePreviousBaseValue = partitionBaseSizeValue.value();

            int newBaseFileCount;
            int newTotalFileCount;
            long newBaseFileSize;
            long newTotalFileSize;
            if (enventOp.equals("delete") && beforeCommitted && committed){
                newTotalFileCount = previousTotalFileCountValue - currentFileCountValue;
                newTotalFileSize = fileBytesSizePreviousTotalValue - currentFileBytesSize;
                partitionTotalFileCountValue.update(newTotalFileCount);
                partitionTotalSizeValue.update(newTotalFileSize);
                // 如果 delete 操作后，totalFileCount 为 0，清空 base 部分
                newBaseFileCount = (newTotalFileCount == 0) ? 0 : previousBaseFileCountValue;
                newBaseFileSize = (newTotalFileCount == 0) ? 0L : fileBytesSizePreviousBaseValue;

                partitionBaseSizeValue.update(newBaseFileSize);
                partitionBaseFileCountValue.update(newBaseFileCount);
                PartitionCounts partitionCounts = new PartitionCounts(tableId,partitionDesc,
                        partitionTotalFileCountValue.value(),
                        partitionBaseFileCountValue.value(),
                        partitionTotalSizeValue.value(),
                        partitionBaseSizeValue.value());
                out.collect(partitionCounts);
            } else {
                if (committed) {
                    newTotalFileCount = previousTotalFileCountValue + currentFileCountValue;
                    newTotalFileSize = fileBytesSizePreviousTotalValue + currentFileBytesSize;
                    newTotalFileCount = Math.max(0, newTotalFileCount);  // 防止负值
                    newTotalFileSize = Math.max(0, newTotalFileSize);
                    if (commitOp.equals("CompactionCommit")){
                        newBaseFileCount = currentFileCountValue;
                        newBaseFileSize = currentFileBytesSize;
                    } else {
                        newBaseFileCount = newTotalFileCount == 0? 0 : previousBaseFileCountValue + currentFileCountValue;
                        newBaseFileSize = newTotalFileCount == 0? 0 : fileBytesSizePreviousBaseValue + currentFileBytesSize;
                    }
                    newBaseFileCount = Math.max(0, newBaseFileCount);  // 防止负值
                    newBaseFileSize = Math.max(0, newBaseFileSize);
                    // 更新累加状态
                    partitionBaseSizeValue.update(newBaseFileSize);
                    partitionBaseFileCountValue.update(newBaseFileCount);
                    partitionTotalFileCountValue.update(newTotalFileCount);
                    partitionTotalSizeValue.update(newTotalFileSize);
                    PartitionCounts partitionCounts = new PartitionCounts(tableId,partitionDesc,partitionTotalFileCountValue.value(),
                            newBaseFileCount,
                            partitionTotalSizeValue.value(),
                            newBaseFileSize);

                    out.collect(partitionCounts);
                }
            }
        }
    }
}
