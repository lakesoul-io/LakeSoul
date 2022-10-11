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

package com.dmetasoul.lakesoul.meta.external.mysql;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.TableNameId;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Table;
import io.debezium.relational.Tables;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MysqlDBManager implements ExternalDBManager {

    private static final String EXTERNAL_MYSQL_TABLE_PREFIX = "external_mysql_table_";
    public static final int DEFAULT_MYSQL_PORT = 3306;
    private final DBConnector dbConnector;

    private final DBManager lakesoulDBManager = new DBManager();
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private final int hashBucketNum;
    private final boolean useCdc;
    private HashSet<String> excludeTables;
    private HashSet<String> includeTables;
    private String[] filterTables = new String[]{"sys_config"};

    MysqlDataTypeConverter converter = new MysqlDataTypeConverter();
    MySqlAntlrDdlParser parser = new MySqlAntlrDdlParser();

    public MysqlDBManager(String dbName,
                          String user,
                          String passwd,
                          String host,
                          String port,
                          HashSet<String> excludeTables,
                          String pathPrefix,
                          int hashBucketNum,
                          boolean useCdc
                          ) {
        this(
                dbName,
                user,
                passwd,
                host,
                port,
                excludeTables,
                new HashSet<>(),
                pathPrefix,
                hashBucketNum,
                useCdc);
    }

    public MysqlDBManager(String dbName,
                          String user,
                          String passwd,
                          String host,
                          String port,
                          HashSet<String> excludeTables,
                          HashSet<String> includeTables,
                          String pathPrefix, int hashBucketNum, boolean useCdc) {
        this.dbName = dbName;
        this.excludeTables = excludeTables;
        this.includeTables = includeTables;
        excludeTables.addAll(Arrays.asList(filterTables));

        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useSSL=false";
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        lakesoulTablePathPrefix = pathPrefix;
        this.hashBucketNum = hashBucketNum;
        this.useCdc = useCdc;
    }


    @Override
    public List<String> listTables() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "show tables";
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString(String.format("Tables_in_%s", dbName));
                if (includeTables.contains(tableName) | !excludeTables.contains(tableName)) {
                    list.add(tableName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }


    @Override
    public void importOrSyncLakeSoulTable(String tableName) {
        if (!includeTables.contains(tableName) && excludeTables.contains(tableName)) {
            System.out.println(String.format("Table %s is excluded by exclude table list", tableName));
            return;
        }
        String mysqlDDL = showCreateTable(tableName);
        Tuple2<StructType, List<String>> schemaAndPK = ddlToSparkSchema(tableName, mysqlDDL);
        if (schemaAndPK.f1.isEmpty()) {
            throw new IllegalStateException(String.format("Table %s has no primary key, table with no Primary Keys is not supported", tableName));
        }

        boolean exists = lakesoulDBManager.isTableExistsByTableName(tableName, dbName);
        if (exists) {
            // sync lakesoul table schema only
            TableNameId tableId = lakesoulDBManager.shortTableName(tableName, dbName);
            String newTableSchema = schemaAndPK.f0.json();

            lakesoulDBManager.updateTableSchema(tableId.getTableId(), newTableSchema);
        } else {
            // import new lakesoul table with schema, pks and properties
            try {
                String tableId = EXTERNAL_MYSQL_TABLE_PREFIX + UUID.randomUUID();

                String qualifiedPath =
                        FlinkUtil.makeQualifiedPath(new Path(new Path(
                                lakesoulTablePathPrefix, dbName
                        ), tableName)).toString();


                String tableSchema = schemaAndPK.f0.json();
                List<String> priKeys = schemaAndPK.f1;
                String partitionsInTableInfo = ";" + String.join(",", priKeys);
                JSONObject json = new JSONObject();
                json.put("hashBucketNum", String.valueOf(hashBucketNum));
                json.put("lakesoul_cdc_change_column", "rowKinds");

                lakesoulDBManager.createNewTable(tableId, dbName, tableName, qualifiedPath,
                        tableSchema,
                        json, partitionsInTableInfo
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            return;
        }
        lakesoulDBManager.createNewNamespace(namespace, new JSONObject(), "");
    }

    public String showCreateTable(String tableName) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("show create table %s", tableName);
        String result = null;
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("Create Table");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return result;
    }

    public Tuple2<StructType, List<String>> ddlToSparkSchema(String tableName, String ddl) {
        final StructType[] stNew = {new StructType()};

        parser.parse(ddl, new Tables());
        Table table = parser.databaseTables().forTable(null, null, tableName);
        table.columns()
                .forEach(col -> {
                    String name = col.name();
                    DataType datatype = converter.schemaBuilder(col);
                    if (datatype == null) {
                        throw new IllegalStateException("Unhandled data types");
                    }
                    stNew[0] = stNew[0].add(name, datatype, col.isOptional());
                });
        //if uescdc add lakesoulcdccolumns
        if (useCdc) {
            stNew[0] = stNew[0].add("rowKinds", DataTypes.StringType, true);
        }
        return Tuple2.of(stNew[0], table.primaryKeyColumnNames());
    }
}
