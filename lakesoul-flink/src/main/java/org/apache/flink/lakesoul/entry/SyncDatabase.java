// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


package org.apache.flink.lakesoul.entry;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;

import java.sql.*;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkDatabasesOptions.*;

public class SyncDatabase {

    public static void main(String[] args) throws SQLException {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String sourceDatabase = parameter.get(SOURCE_DB_DB_NAME.key());
        String sourceTableName = parameter.get(SOURCE_DB_LAKESOUL_TABLE.key()).toLowerCase();
        String targeSyncName = parameter.get(TARGET_DATABASE_TYPE.key());
        String targetDatabase = parameter.get(TARGET_DB_DB_NAME.key());
        String targetTableName = parameter.get(TARGET_DB_TABLE_NAME.key()).toLowerCase();
        String url = parameter.get(TARGET_DB_URL.key());
        String username = parameter.get(TARGET_DB_USER.key());
        String password = parameter.get(TARGET_DB_PASSWORD.key());
        int sinkParallelism = parameter.getInt(SINK_PARALLELISM.key(), SINK_PARALLELISM.defaultValue());
        boolean useBatch = parameter.getBoolean(BATHC_STREAM_SINK.key(), BATHC_STREAM_SINK.defaultValue());
        //int replicationNum = parameter.getInt(DORIS_REPLICATION_NUM.key(), DORIS_REPLICATION_NUM.defaultValue());

        String fenodes = parameter.get(DORIS_FENODES.key(), DORIS_FENODES.defaultValue());
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(sinkParallelism);

        switch (targeSyncName) {
            case "mysql":
                xsyncToMysql(env, useBatch, url, sourceDatabase, username, password, targetDatabase, sourceTableName, targetTableName);
                break;
            case "postgres":
                xsyncToPg(env, useBatch, url, sourceDatabase, username, password, targetDatabase, sourceTableName, targetTableName);
                break;
            case "doris":
                xsyncToDoris(env, useBatch, url, sourceDatabase, username, password, targetDatabase, sourceTableName, targetTableName, fenodes);
                break;
            default:
                throw new RuntimeException("not supported the database: " + targeSyncName);
        }
    }

    public static String pgAndMsqlCreateTableSql(String[] stringFieldTypes, String[] fieldNames, String targetTableName, String pk) {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(targetTableName)
                .append(" (");
        for (int i = 0; i < fieldNames.length; i++) {
            String dataType = stringFieldTypes[i];
            //String nullable = stringFieldTypes[i].contains("NULL") ? "" : " NOT NULL";
            createTableQuery.append(fieldNames[i]).append(" ").append(dataType);
            if (i != fieldNames.length - 1) {
                createTableQuery.append(", ");
            }
        }
        if (pk!=null){
            createTableQuery.append(" ,PRIMARY KEY(").append(pk);
            createTableQuery.append(")");
        }
        createTableQuery.append(")");
        return createTableQuery.toString();
    }

    public static String[] getMysqlFieldsTypes(DataType[] fieldTypes, String[] fieldNames, String pk) {
        String[] stringFieldTypes = new String[fieldTypes.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getLogicalType() instanceof VarCharType) {
                String mysqlType = "TEXT";
                if (pk!=null){
                    if (pk.contains(fieldNames[i])) {
                        mysqlType = "VARCHAR(100)";
                    }
                }
                stringFieldTypes[i] = mysqlType;
            } else if (fieldTypes[i].getLogicalType() instanceof DecimalType) {
                stringFieldTypes[i] = "FLOAT";
            } else if (fieldTypes[i].getLogicalType() instanceof BinaryType) {
                stringFieldTypes[i] = "BINARY";
            } else if (fieldTypes[i].getLogicalType() instanceof LocalZonedTimestampType | fieldTypes[i].getLogicalType() instanceof TimestampType) {
                stringFieldTypes[i] = "TIMESTAMP";
            } else if (fieldTypes[i].getLogicalType() instanceof BooleanType) {
                stringFieldTypes[i] = "BOOLEAN";
            } else {
                stringFieldTypes[i] = fieldTypes[i].toString();
            }
        }
        return stringFieldTypes;
    }

    public static String[] getPgFieldsTypes(DataType[] fieldTypes, String[] fieldNames, String pk) {
        String[] stringFieldTypes = new String[fieldTypes.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getLogicalType() instanceof VarCharType) {
                String mysqlType = "TEXT";
                if (pk!=null){
                    if (pk.contains(fieldNames[i])) {
                        mysqlType = "VARCHAR(100)";
                    }
                }
                stringFieldTypes[i] = mysqlType;
            } else if (fieldTypes[i].getLogicalType() instanceof DoubleType) {
                stringFieldTypes[i] = "FLOAT8";
            } else if (fieldTypes[i].getLogicalType() instanceof FloatType) {
                stringFieldTypes[i] = "FLOAT4";
            } else if (fieldTypes[i].getLogicalType() instanceof BinaryType) {
                stringFieldTypes[i] = "BYTEA";
            } else if (fieldTypes[i].getLogicalType() instanceof LocalZonedTimestampType | fieldTypes[i].getLogicalType() instanceof TimestampType) {
                stringFieldTypes[i] = "TIMESTAMP";
            } else {
                stringFieldTypes[i] = fieldTypes[i].toString();
            }
        }
        return stringFieldTypes;
    }

    public static String[] getDorisFieldTypes(DataType[] fieldTypes) {
        String[] stringFieldTypes = new String[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getLogicalType() instanceof TimestampType){
                stringFieldTypes[i] = "DATETIME";
            }
            else if (fieldTypes[i].getLogicalType() instanceof VarCharType){
                stringFieldTypes[i] = "VARCHAR";

            }   else {
                stringFieldTypes[i] = fieldTypes[i].toString();
            }
        }
        return stringFieldTypes;
    }

    public static String getTablePk(String sourceDataBae, String sourceTableName) {
        DBManager dbManager = new DBManager();
        TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(sourceTableName, sourceDataBae);
        String tableProperties = tableInfo.getProperties();
        JSONObject jsonObject = JSON.parseObject(tableProperties);
        return jsonObject.getString("hashPartitions");
    }

    public static void xsyncToPg(StreamExecutionEnvironment env, boolean bathXync, String url, String sourceDatabase, String username, String password, String targetDatabase, String sourceTableName, String targetTableName) throws SQLException {
        if (bathXync) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase(sourceDatabase);
        String jdbcUrl = url + targetDatabase;
        Connection conn = DriverManager.getConnection(jdbcUrl, username, password);

        TableResult schemaResult = tEnvs.executeSql(
                "SELECT * FROM lakeSoul.`" + sourceDatabase + "`.`" + sourceTableName + "` LIMIT 1");

        String[] fieldNames = schemaResult.getTableSchema().getFieldNames();
        DataType[] fieldTypes = schemaResult.getTableSchema().getFieldDataTypes();
        String tablePk = getTablePk(sourceDatabase, sourceTableName);
        String[] stringFieldsTypes = getPgFieldsTypes(fieldTypes, fieldNames, tablePk);

        String createTableSql = pgAndMsqlCreateTableSql(stringFieldsTypes, fieldNames, targetTableName, tablePk);
        Statement statement = conn.createStatement();
        // Create the target table in MySQL
        statement.executeUpdate(createTableSql.toString());
        String createCatalog = "create catalog postgres_catalog with('type'='jdbc','default-database'=" + "'" + targetDatabase + "'" + "," + "'username'=" +
                "'" + username + "'" + "," + "'password'=" + "'" + password + "'" + "," + "'base-url'=" + "'" + url + "'" + ")";
        // Move data from LakeSoul to MySQL
        tEnvs.executeSql(createCatalog);
        String insertQuery = "INSERT INTO postgres_catalog.`" + targetDatabase+ "`.`" + targetTableName +
                "` SELECT * FROM lakeSoul.`"  +sourceDatabase + "`.`" + sourceTableName + "`";

        tEnvs.executeSql(insertQuery);
        statement.close();
        conn.close();
    }

    public static void xsyncToMysql(StreamExecutionEnvironment env, boolean bathXync, String url, String sourceDatabase, String username, String password, String targetDatabase, String sourceTableName, String targetTableName) throws SQLException {
        if (bathXync) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        tEnvs.useCatalog("lakeSoul");
        tEnvs.useDatabase(sourceDatabase);

        TableResult schemaResult = tEnvs.executeSql(
                "SELECT * FROM lakeSoul.`" + sourceDatabase + "`.`" + sourceTableName + "` LIMIT 1");

        String[] fieldNames = schemaResult.getTableSchema().getFieldNames();
        DataType[] fieldTypes = schemaResult.getTableSchema().getFieldDataTypes();
        String tablePk = getTablePk(sourceDatabase, sourceTableName);
        String[] stringFieldsTypes = getMysqlFieldsTypes(fieldTypes, fieldNames, tablePk);

        String createTableSql = pgAndMsqlCreateTableSql(stringFieldsTypes, fieldNames, targetTableName, tablePk);
        String newUrl = url + targetDatabase;
        Connection conn = DriverManager.getConnection(newUrl, username, password);
        Statement statement = conn.createStatement();

        // Create the target table in MySQL
        statement.executeUpdate(createTableSql.toString());
        String createCatalog = "create catalog mysql_catalog with('type'='jdbc','default-database'=" + "'" + targetDatabase + "'" + "," + "'username'=" +
                "'" + username + "'" + "," + "'password'=" + "'" + password + "'" + "," + "'base-url'=" + "'" + url + "'" + ")";
        // Move data from LakeSoul to MySQL
        tEnvs.executeSql(createCatalog);
        String insertQuery = "INSERT INTO mysql_catalog.`" + targetDatabase + "`.`" + targetTableName+"`" +
                " SELECT * FROM lakeSoul.`" + sourceDatabase + "`.`" + sourceTableName + "`";

        tEnvs.executeSql(insertQuery);
        // Close connections
        statement.close();
        conn.close();
    }

    public static void xsyncToDoris(StreamExecutionEnvironment env, boolean batchXync, String url, String sourceDatabase, String username, String password, String targetDatabase, String sourceTableName, String targetTableName, String fenodes) throws SQLException {
        if (batchXync) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        String jdbcUrl = url + targetDatabase;
        TableResult schemaResult = tEnvs.executeSql(
                "SELECT * FROM lakeSoul.`" + sourceDatabase + "`.`" + sourceTableName + "` LIMIT 1");
        DataType[] fieldDataTypes = schemaResult.getTableSchema().getFieldDataTypes();
        String[] dorisFieldTypes = getDorisFieldTypes(fieldDataTypes);
        String[] fieldNames = schemaResult.getTableSchema().getFieldNames();

        StringBuilder coulmns = new StringBuilder();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            coulmns.append("`").append(fieldNames[i]).append("` ").append(dorisFieldTypes[i]);
            if (i< fieldDataTypes.length-1){
                coulmns.append(",");
            }
        }
        String sql = String.format(
                "create table %s(%s) with ('connector' = '%s', 'jdbc-url' = '%s', 'fenodes' = '%s', 'table.identifier' = '%s', 'username' = '%s', 'password' = '%s')",
                targetTableName, coulmns, "doris", jdbcUrl, fenodes, targetDatabase + "." + targetTableName, username, password);
        tEnvs.executeSql(sql);
        tEnvs.executeSql("insert into "+targetTableName+" select * from lakeSoul.`"+sourceDatabase+"`."+sourceTableName);
    }
}