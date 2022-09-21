package org.apache.flink.lakeSoul.sink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.List;
import java.util.Locale;

public class LakeSoulDDLSink extends RichSinkFunction<JsonSourceRecord> {
    private final String ddlField = "ddl";
    private final String historyField = "historyRecord";
//    private final String tableChanges = "tableChanges";
    private final String source = "source";
    private final String table = "table";
//    private final String primarykey = "primaryKeyColumnNames";
//    private final String columns = "columns";
    private final String dbName = "db";

    @Override
    public void invoke(JsonSourceRecord value, Context context) throws Exception {
        Struct val = (Struct) value.getValue(SourceRecordJsonSerde.getInstance()).value();
        String history = val.getString(historyField);
        JSONObject jso = (JSONObject) JSON.parse(history);
        String ddlval = jso.getString(ddlField).toLowerCase();
        Struct sourceItem = (Struct)val.get(source);
        String tablename=sourceItem.getString(table);
        String dataBasename=sourceItem.getString(dbName);
        ParameterTool pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        if(ddlval.contains("alter")){
            System.out.println("alter");
            System.out.println(pt.get("user"));
//           JSONArray tables = jso.getJSONArray(tableChanges);
//           for(int i=0;i<tables.size();i++){
//               JSONObject tableItem = tables.getJSONObject(i).getJSONObject(table);
//               JSONArray keys = tableItem.getJSONArray(primarykey);
//               JSONArray cols = tableItem.getJSONArray(columns);
 //          }
        }
        if(ddlval.contains("create")){
            System.out.println("create");
            System.out.println(pt.get("password"));

        }
    }
}
