// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions;
import org.apache.flink.lakesoul.types.BinarySourceRecord;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class LakeSoulDDLSink extends RichSinkFunction<BinarySourceRecord> {
    private static final String ddlField = "ddl";
    private static final String historyField = "historyRecord";
    private static final String source = "source";
    private static final String table = "table";


    @Override
    public void invoke(BinarySourceRecord value, Context context) throws Exception {
        Struct val = (Struct) value.getDDLStructValue().value();
        String history = val.getString(historyField);
        JSONObject jso = (JSONObject) JSON.parse(history);
        String ddlval = jso.getString(ddlField).toLowerCase();
        Struct sourceItem = (Struct) val.get(source);
        String tablename = sourceItem.getString(table);
        Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        List<String> excludeTablesList = Arrays.asList(globalParams.getOrDefault(LakeSoulDDLSinkOptions.SOURCE_DB_EXCLUDE_TABLES.key(),
                LakeSoulDDLSinkOptions.SOURCE_DB_EXCLUDE_TABLES.defaultValue()).split(","));
        HashSet<String> excludeTables = new HashSet<>(excludeTablesList);
        MysqlDBManager mysqlDbManager = new MysqlDBManager(globalParams.get(LakeSoulDDLSinkOptions.SOURCE_DB_DB_NAME.key()),
                globalParams.get(LakeSoulDDLSinkOptions.SOURCE_DB_USER.key()),
                globalParams.get(LakeSoulDDLSinkOptions.SOURCE_DB_PASSWORD.key()),
                globalParams.get(LakeSoulDDLSinkOptions.SOURCE_DB_HOST.key()),
                globalParams.getOrDefault(LakeSoulDDLSinkOptions.SOURCE_DB_PORT.key(),
                        Integer.toString(MysqlDBManager.DEFAULT_MYSQL_PORT)),
                excludeTables,
                globalParams.get(LakeSoulDDLSinkOptions.WAREHOUSE_PATH.key()),
                Integer.parseInt(globalParams.get(LakeSoulDDLSinkOptions.BUCKET_PARALLELISM.key())),
                Boolean.parseBoolean(globalParams.get(LakeSoulDDLSinkOptions.USE_CDC.key())));
        if (ddlval.contains("alter table") || ddlval.contains("create table")) {
            mysqlDbManager.importOrSyncLakeSoulTable(tablename);
        }
    }
}
