package org.apache.flink.lakeSoul.sink;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.List.of;

public class LakeSoulDDLSink extends RichSinkFunction<JsonSourceRecord> {
    private final String ddlField = "ddl";
    private final String historyField = "historyRecord";
    private final String source = "source";
    private final String table = "table";
    private final String dbName = "db";
    private DBManager lakesoulDbManager;
    private MysqlDBManager mysqlDbManager;

    public static class ConfKey {
        public static String DB_NAME = "dbName";
        public static String DB_USER = "user";
        public static String DB_PASSWORD = "password";
        public static String DB_HOST = "host";
        public static String DB_PORT = "port";
        public static String DB_EXCLUDE_TABLE = "excludeTables";
        public static String LAKESOUL_TABLE_PATH_PREFIX = "path_prefix";
    }

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
        lakesoulDbManager = new DBManager();
        List<String> excludeTablesList = Arrays.asList(pt.get(ConfKey.DB_EXCLUDE_TABLE, "").split(","));
        HashSet<String> excludeTables = new HashSet<>(excludeTablesList);
        mysqlDbManager = new MysqlDBManager(pt.get(ConfKey.DB_NAME),
                pt.get(ConfKey.DB_USER),
                pt.get(ConfKey.DB_PASSWORD),
                pt.get(ConfKey.DB_HOST),
                Integer.toString(pt.getInt(ConfKey.DB_PORT, MysqlDBManager.DEFAULT_MYSQL_PORT)),
                excludeTables,
                pt.get(ConfKey.LAKESOUL_TABLE_PATH_PREFIX));
        if(ddlval.contains("alter")||ddlval.contains("create")){
            mysqlDbManager.importOrSyncLakeSoulTable(tablename);
        }

    }
}
