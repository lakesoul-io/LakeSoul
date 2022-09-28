/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.lakesoul.types.SourceRecordJsonSerde;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class LakeSoulDDLSink extends RichSinkFunction<JsonSourceRecord> {
    private static final String ddlField = "ddl";
    private static final String historyField = "historyRecord";
    private static final String source = "source";
    private static final String table = "table";


    @Override
    public void invoke(JsonSourceRecord value, Context context) throws Exception {
        Struct val = (Struct) value.getValue(SourceRecordJsonSerde.getInstance()).value();
        String history = val.getString(historyField);
        JSONObject jso = (JSONObject) JSON.parse(history);
        String ddlval = jso.getString(ddlField).toLowerCase();
        Struct sourceItem = (Struct) val.get(source);
        String tablename = sourceItem.getString(table);
        ParameterTool pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        List<String> excludeTablesList = Arrays.asList(pt.get(LakeSoulDDLSinkOptions.SOURCE_DB_EXCLUDE_TABLES.key(), LakeSoulDDLSinkOptions.SOURCE_DB_EXCLUDE_TABLES.defaultValue()).split(","));
        HashSet<String> excludeTables = new HashSet<>(excludeTablesList);
        MysqlDBManager mysqlDbManager = new MysqlDBManager(pt.get(LakeSoulDDLSinkOptions.SOURCE_DB_DB_NAME.key()),
                                            pt.get(LakeSoulDDLSinkOptions.SOURCE_DB_USER.key()),
                                            pt.get(LakeSoulDDLSinkOptions.SOURCE_DB_PASSWORD.key()),
                                            pt.get(LakeSoulDDLSinkOptions.SOURCE_DB_HOST.key()),
                                            Integer.toString(pt.getInt(LakeSoulDDLSinkOptions.SOURCE_DB_PORT.key(),
                                                    MysqlDBManager.DEFAULT_MYSQL_PORT)),
                                            excludeTables,
                                            pt.get(LakeSoulDDLSinkOptions.WAREHOUSE_PATH.key()),
                                            pt.getInt(LakeSoulDDLSinkOptions.BUCKET_PARALLELISM.key()),
                                            pt.getBoolean(LakeSoulDDLSinkOptions.USE_CDC.key()));
        if (ddlval.contains("alter") || ddlval.contains("create")) {
            mysqlDbManager.importOrSyncLakeSoulTable(tablename);
        }
    }
}
