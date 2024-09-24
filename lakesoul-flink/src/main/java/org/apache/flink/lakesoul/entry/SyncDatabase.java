// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry;

import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.lakesoul.entry.sql.flink.LakeSoulInAndOutputJobListener;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.tool.JobOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.table.api.Table;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.flink.lakesoul.entry.MongoSinkUtils.*;
import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkDatabasesOptions.*;

public class SyncDatabase {

    static String targetTableName;
    static String dbType;
    static String sourceDatabase;
    static String sourceTableName;
    static String targetDatabase;
    static String url;
    static String username;
    static String password;
    static boolean useBatch;
    static int sinkParallelism;
    static String jdbcOrDorisOptions;
    static int checkpointInterval;
    static LakeSoulInAndOutputJobListener listener;
    static String lineageUrl = null;

    public static void main(String[] args) throws Exception {
        StringBuilder connectorOptions = new StringBuilder();
        ParameterTool parameter = ParameterTool.fromArgs(args);
        sourceDatabase = parameter.get(SOURCE_DB_DB_NAME.key());
        sourceTableName = parameter.get(SOURCE_DB_LAKESOUL_TABLE.key()).toLowerCase();
        dbType = parameter.get(TARGET_DATABASE_TYPE.key());
        targetDatabase = parameter.get(TARGET_DB_DB_NAME.key());
        targetTableName = parameter.get(TARGET_DB_TABLE_NAME.key()).toLowerCase();
        url = parameter.get(TARGET_DB_URL.key());
        checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(), JOB_CHECKPOINT_INTERVAL.defaultValue());
        if (dbType.equals("mysql") || dbType.equals("postgresql") || dbType.equals("doris")){
            for (int i = 0; i < args.length; i++) {
                if ( args[i].startsWith("--D")){
                    connectorOptions.append("'")
                            .append(args[i].substring(3))
                            .append("'")
                            .append("=")
                            .append("'")
                            .append(args[i+1])
                            .append("'")
                            .append(",");
                }
            }
            if (connectorOptions.length()>0){
                jdbcOrDorisOptions = connectorOptions.substring(0, connectorOptions.length() - 1);
            }
        }
        if (!dbType.equals("mongodb")) {
            username = parameter.get(TARGET_DB_USER.key());
            password = parameter.get(TARGET_DB_PASSWORD.key());
        }
        sinkParallelism = parameter.getInt(SINK_PARALLELISM.key(), SINK_PARALLELISM.defaultValue());
        useBatch = parameter.getBoolean(BATHC_STREAM_SINK.key(), BATHC_STREAM_SINK.defaultValue());
        Configuration conf = new Configuration();
        conf.setString(RestOptions.BIND_PORT, "8081-8089");
        StreamExecutionEnvironment env = null;
        lineageUrl = System.getenv("LINEAGE_URL");
        if (lineageUrl != null) {
            conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
            conf.set(JobOptions.transportTypeOption, "http");
            conf.set(JobOptions.urlOption, lineageUrl);
            conf.set(JobOptions.execAttach, true);
            env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
            String appName = env.getConfiguration().get(JobOptions.KUBE_CLUSTER_ID);
            String namespace = System.getenv("LAKESOUL_CURRENT_DOMAIN");
            if (namespace == null) {
                namespace = "public";
            }
            if (useBatch) {
                listener = new LakeSoulInAndOutputJobListener(lineageUrl, "BATCH");
            } else {
                listener = new LakeSoulInAndOutputJobListener(lineageUrl);
            }
            listener.jobName(appName, namespace);
            env.registerJobListener(listener);
        } else {
            env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        }
        env.setParallelism(sinkParallelism);
        switch (dbType) {
            case "mysql":
                xsyncToMysql(env);
                break;
            case "postgres":
                xsyncToPg(env);
                break;
            case "doris":
                String fenodes = parameter.get(DORIS_FENODES.key(), DORIS_FENODES.defaultValue());
                xsyncToDoris(env, fenodes);
                break;
            case "mongodb":
                String uri = parameter.get(MONGO_DB_URI.key());
                int batchSize = parameter.getInt(BATCH_SIZE.key(), BATCH_SIZE.defaultValue());
                int batchIntervalMs = parameter.getInt(BATCH_INTERVAL_MS.key(), BATCH_INTERVAL_MS.defaultValue());
                xsyncToMongodb(env, uri, batchSize, batchIntervalMs);
                break;
            default:
                throw new RuntimeException("not supported the database: " + dbType);
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
        if (pk != null) {
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
                if (pk != null) {
                    if (pk.contains(fieldNames[i])) {
                        mysqlType = "VARCHAR(255)";
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
            } else if (fieldTypes[i].getLogicalType() instanceof VarBinaryType) {
                stringFieldTypes[i] = "BLOB";
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
                if (pk != null) {
                    if (pk.contains(fieldNames[i])) {
                        mysqlType = "VARCHAR(255)";
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
            } else if (fieldTypes[i].getLogicalType() instanceof VarBinaryType) {
                stringFieldTypes[i] = "BYTEA";
            } else {
                stringFieldTypes[i] = fieldTypes[i].toString();
            }
        }
        return stringFieldTypes;
    }

    public static String[] getDorisFieldTypes(DataType[] fieldTypes) {
        String[] stringFieldTypes = new String[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            if (fieldTypes[i].getLogicalType() instanceof TimestampType) {
                stringFieldTypes[i] = "TIMESTAMP";
            } else if (fieldTypes[i].getLogicalType() instanceof VarCharType) {
                stringFieldTypes[i] = "VARCHAR";
            } else if (fieldTypes[i].getLogicalType() instanceof LocalZonedTimestampType ) {
                stringFieldTypes[i] = "TIMESTAMP";
            } else {
                stringFieldTypes[i] = fieldTypes[i].toString();
            }
        }
        return stringFieldTypes;
    }

    public static String getTablePk(String sourceDataBae, String sourceTableName) {
        DBManager dbManager = new DBManager();
        TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(sourceTableName, sourceDataBae);
        String partitions = tableInfo.getPartitions();
        DBUtil.TablePartitionKeys keys = DBUtil.parseTableInfoPartitions(partitions);

        List<String> primaryKeys = keys.primaryKeys;
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < primaryKeys.size(); i++) {
            stringBuilder.append(primaryKeys.get(i));
            if (i < primaryKeys.size() - 1) {
                stringBuilder.append(",");
            }
        }
        return primaryKeys.size() == 0 ? null : stringBuilder.toString();
    }

    public static String getTableDomain(String sourceDataBae, String sourceTableName) {
        DBManager dbManager = new DBManager();
        TableInfo tableInfo = dbManager.getTableInfoByNameAndNamespace(sourceTableName, sourceDataBae);
        return tableInfo.getDomain();
    }

    public static void xsyncToPg(StreamExecutionEnvironment env) throws SQLException {
        if (useBatch) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        String jdbcUrl = url + targetDatabase;
        Connection conn = DriverManager.getConnection(jdbcUrl, username, password);

        Table lakesoulTable = tEnvs.from("`lakeSoul`.`" + sourceDatabase + "`.`" + sourceTableName + "`");
        DataType[] fieldDataTypes = lakesoulTable.getSchema().getFieldDataTypes();
        String[] fieldNames = lakesoulTable.getSchema().getFieldNames();
        String tablePk = getTablePk(sourceDatabase, sourceTableName);
        String[] stringFieldsTypes = getPgFieldsTypes(fieldDataTypes, fieldNames, tablePk);

        String createTableSql = pgAndMsqlCreateTableSql(stringFieldsTypes, fieldNames, targetTableName, tablePk);
        Statement statement = conn.createStatement();
        statement.executeUpdate(createTableSql.toString());
        StringBuilder coulmns = new StringBuilder();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            switch (stringFieldsTypes[i]) {
                case "BYTEA":
                    coulmns.append("`").append(fieldNames[i]).append("` ").append("BYTES");
                    break;
                case "TEXT":
                    coulmns.append("`").append(fieldNames[i]).append("` ").append("VARCHAR");
                    break;
                case "FLOAT8":
                    coulmns.append("`").append(fieldNames[i]).append("`").append("DOUBLE");
                    break;
                default:
                    coulmns.append("`").append(fieldNames[i]).append("` ").append(stringFieldsTypes[i]);
                    break;
            }
            if (i < fieldDataTypes.length - 1) {
                coulmns.append(",");
            }
        }
        String sql;
        if (jdbcOrDorisOptions==null){
            if (tablePk != null) {
                sql = String.format(
                        "create table %s(%s ,PRIMARY KEY (%s) NOT ENFORCED) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s')",
                        targetTableName, coulmns, tablePk, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism);
            } else {
                sql = String.format("create table %s(%s) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s')",
                        targetTableName, coulmns, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism);
            }
        }else {
            if (tablePk != null) {
                sql = String.format(
                        "create table %s(%s ,PRIMARY KEY (%s) NOT ENFORCED) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s', %s)",
                        targetTableName, coulmns, tablePk, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism, jdbcOrDorisOptions);
            } else {
                sql = String.format("create table %s(%s) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s', %s)",
                        targetTableName, coulmns, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism, jdbcOrDorisOptions);
            }
        }
        tEnvs.executeSql(sql);
        tEnvs.executeSql("insert into " + targetTableName + " select * from lakeSoul.`" + sourceDatabase + "`." + sourceTableName);
        statement.close();
        conn.close();
    }


    public static void xsyncToMysql(StreamExecutionEnvironment env) throws SQLException {
        if (useBatch) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        String jdbcUrl = url + targetDatabase;
        Table lakesoulTable = tEnvs.from("`lakeSoul`.`" + sourceDatabase + "`.`" + sourceTableName + "`");
        DataType[] fieldDataTypes = lakesoulTable.getSchema().getFieldDataTypes();
        String[] fieldNames = lakesoulTable.getSchema().getFieldNames();
        String tablePk = getTablePk(sourceDatabase, sourceTableName);
        String[] stringFieldsTypes = getMysqlFieldsTypes(fieldDataTypes, fieldNames, tablePk);
        String createTableSql = pgAndMsqlCreateTableSql(stringFieldsTypes, fieldNames, targetTableName, tablePk);

        Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
        Statement statement = conn.createStatement();
        // Create the target table in MySQL
        statement.executeUpdate(createTableSql.toString());
        StringBuilder coulmns = new StringBuilder();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            if (stringFieldsTypes[i].equals("BLOB")) {
                coulmns.append("`").append(fieldNames[i]).append("` ").append("BYTES");
            } else if (stringFieldsTypes[i].equals("TEXT")) {
                coulmns.append("`").append(fieldNames[i]).append("` ").append("VARCHAR");

            } else {
                coulmns.append("`").append(fieldNames[i]).append("` ").append(stringFieldsTypes[i]);
            }
            if (i < fieldDataTypes.length - 1) {
                coulmns.append(",");
            }
        }
        String sql;
        if (jdbcOrDorisOptions==null){
            if (tablePk != null) {
                sql = String.format(
                        "create table %s(%s ,PRIMARY KEY (%s) NOT ENFORCED) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s')",
                        targetTableName, coulmns, tablePk, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism);
            } else {
                sql = String.format("create table %s(%s) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s')",
                        targetTableName, coulmns, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism);
            }
        }else {
            if (tablePk != null) {
                sql = String.format(
                        "create table %s(%s ,PRIMARY KEY (%s) NOT ENFORCED) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s', %s)",
                        targetTableName, coulmns, tablePk, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism, jdbcOrDorisOptions);
            } else {
                sql = String.format("create table %s(%s) with ('connector' = '%s', 'url' = '%s', 'table-name' = '%s', 'username' = '%s', 'password' = '%s', 'sink.parallelism' = '%s', %s)",
                        targetTableName, coulmns, "jdbc", jdbcUrl, targetTableName, username, password, sinkParallelism, jdbcOrDorisOptions);
            }
        }

        tEnvs.executeSql(sql);
        tEnvs.executeSql("insert into " + targetTableName + " select * from lakeSoul.`" + sourceDatabase + "`." + sourceTableName);

        statement.close();
        conn.close();
    }

    public static void xsyncToDoris(StreamExecutionEnvironment env, String fenodes) throws Exception {
        if (useBatch) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        String jdbcUrl = url + targetDatabase;
        Table lakesoulTable = tEnvs.from("`lakeSoul`.`" + sourceDatabase + "`.`" + sourceTableName + "`");
        DataType[] fieldDataTypes = lakesoulTable.getSchema().getFieldDataTypes();
        String[] fieldNames = lakesoulTable.getSchema().getFieldNames();
        String[] dorisFieldTypes = getDorisFieldTypes(fieldDataTypes);
        if (lineageUrl != null) {
            String inputName = "lakeSoul." + sourceDatabase + "." + sourceTableName;
            String inputnNamespace = getTableDomain(sourceDatabase,sourceTableName);
            String[] inputTypes = Arrays.stream(fieldDataTypes).map(type -> type.toString()).collect(Collectors.toList()).toArray(new String[0]);
            listener.inputFacets(inputName,inputnNamespace,fieldNames,inputTypes);
            String targetName = "doris." + targetDatabase + "." + targetTableName;
            listener.outputFacets(targetName,"lake-public",fieldNames,dorisFieldTypes);
        }
        StringBuilder coulmns = new StringBuilder();
        for (int i = 0; i < fieldDataTypes.length; i++) {
            coulmns.append("`").append(fieldNames[i]).append("` ").append(dorisFieldTypes[i]);
            if (i < fieldDataTypes.length - 1) {
                coulmns.append(",");
            }
        }
        String sql;
        if (jdbcOrDorisOptions == null){
            sql = String.format(
                    "create table %s(%s) with ('connector' = '%s'," +
                            " 'jdbc-url' = '%s'," +
                            " 'fenodes' = '%s'," +
                            " 'table.identifier' = '%s'," +
                            " 'username' = '%s'," +
                            " 'password' = '%s'," +
                            " 'sink.properties.format' = 'json'," +
                            " 'sink.properties.read_json_by_line' = 'true')",
                    targetTableName, coulmns, "doris", jdbcUrl, fenodes, targetDatabase + "." + targetTableName, username, password);
        }else {
            sql = String.format(
                    "create table %s(%s) with ('connector' = '%s'," +
                            " 'jdbc-url' = '%s'," +
                            " 'fenodes' = '%s'," +
                            " 'table.identifier' = '%s'," +
                            " 'username' = '%s'," +
                            " 'password' = '%s'," +
                            " 'sink.properties.format' = 'json'," +
                            " 'sink.properties.read_json_by_line' = 'true'," +
                            "  %s)",
                    targetTableName, coulmns, "doris", jdbcUrl, fenodes, targetDatabase + "." + targetTableName, username, password, jdbcOrDorisOptions);
        }

        tEnvs.executeSql(sql);
        if (lineageUrl != null){
            String insertsql = "insert into " + targetTableName + " select * from lakeSoul.`" + sourceDatabase + "`." + sourceTableName;
            StreamStatementSet statements =  tEnvs.createStatementSet();
            statements.addInsertSql(insertsql);
            statements.attachAsDataStream();
            env.execute();
        }else{
            tEnvs.executeSql("insert into " + targetTableName + " select * from lakeSoul.`" + sourceDatabase + "`." + sourceTableName);

        }
    }

    public static void xsyncToMongodb(StreamExecutionEnvironment env,
                                      String uri,
                                      int batchSize,
                                      int batchInservalMs) throws Exception {
        createMongoColl(targetDatabase, targetTableName, uri);
        if (useBatch) {
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        } else {
            env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        Catalog lakesoulCatalog = new LakeSoulCatalog();
        tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
        coll = tEnvs.sqlQuery("select * from lakeSoul.`" + sourceDatabase + "`.`" + sourceTableName + "`");
        tEnvs.registerTable("mongodbTbl", coll);
        Table table = tEnvs.sqlQuery("select * from mongodbTbl");
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tEnvs.toRetractStream(table, Row.class);
        MongoSink<Tuple2<Boolean, Row>> sink = MongoSink.<Tuple2<Boolean, Row>>builder()
                .setUri(uri)
                .setDatabase(targetDatabase)
                .setCollection(targetTableName)
                .setBatchSize(batchSize)
                .setBatchIntervalMs(batchInservalMs)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema(new MyMongoSerializationSchema())
                .build();
        rowDataStream.sinkTo(sink).setParallelism(sinkParallelism);
        env.execute();
    }
}
