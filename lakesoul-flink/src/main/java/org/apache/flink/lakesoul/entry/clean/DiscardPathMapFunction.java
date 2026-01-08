package org.apache.flink.lakesoul.entry.clean;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class DiscardPathMapFunction implements MapFunction<String, Tuple2<String, Long>> {

    @Override
    public Tuple2<String, Long> map(String value) throws Exception {

        JSONObject parse = (JSONObject) JSONObject.parse(value);
        String PGtableName = parse.get("tableName").toString();
        JSONObject commitJson = null;
        Tuple2<String, Long> filePathInfo = new Tuple2<>();
        if (PGtableName.equals("discard_compressed_file_info")){
            if (!parse.getJSONObject("after").isEmpty()){
                commitJson = (JSONObject) parse.get("after");
            } else {
                commitJson = (JSONObject) parse.get("before");
            }
        }
        String eventOp = parse.getString("commitOp");
        if (!eventOp.equals("delete")){
            String filePath = commitJson.getString("file_path");
            long timestamp = commitJson.getLong("timestamp");
            filePathInfo.f0 = filePath;
            filePathInfo.f1 = timestamp;
            return filePathInfo;
        } else {
            filePathInfo.f0 = commitJson.getString("file_path");
            filePathInfo.f1 = -5L;
            return filePathInfo;
        }
    }
}
