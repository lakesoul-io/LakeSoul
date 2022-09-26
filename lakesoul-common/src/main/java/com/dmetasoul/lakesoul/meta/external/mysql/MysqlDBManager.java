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
import io.debezium.relational.Tables;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.*;


public class MysqlDBManager implements ExternalDBManager {

    private static final String EXTERNAL_MYSQL_TABLE_PREFIX = "external_mysql_table_";

    public static final int DEFAULT_MYSQL_PORT = 3306;

    public static final String DEFAULT_LAKESOUL_TABLE_PATH_PREFIX = "file://";
    private final DBConnector dbConnector;

    private final DBManager lakesoulDBManager;
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private HashSet<String> excludeTables;
    private String[] filterTables = new String[]{"sys_config"};

    MysqlDataTypeConverter converter;

    MySqlAntlrDdlParser parser;

    public MysqlDBManager(String dbName,
                          String user,
                          String passwd,
                          String host,
                          String port,
                          HashSet<String> excludeTables,
                          String pathPrefix) {
        this.dbName = dbName;
        this.excludeTables = excludeTables;
        excludeTables.addAll(Arrays.asList(filterTables));

        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useSSL=false";
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        lakesoulDBManager = new DBManager();

        converter = new MysqlDataTypeConverter();

        parser = new MySqlAntlrDdlParser();

        lakesoulTablePathPrefix = pathPrefix;
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
                list.add(tableName);
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
        String mysqlDDL = showCreateTable(tableName);

        boolean exists = lakesoulDBManager.isTableExistsByTableName(tableName);
        if (exists) {
            // sync lakesoul table schema
            TableNameId tableId = lakesoulDBManager.shortTableName(tableName);
            String newTableSchema = ddlToSparkSchema(tableName, mysqlDDL).json();

            lakesoulDBManager.updateTableSchema(tableId.getTableId(), newTableSchema);
        } else {
            // import lakesoul table
            String tableId = EXTERNAL_MYSQL_TABLE_PREFIX + UUID.randomUUID();
            System.out.println(tableId);

            String qualifiedPath = StringUtils.join(new String[]{lakesoulTablePathPrefix, dbName, tableName}, '/');;

            String ddl = showCreateTable(tableName);
            String tableSchema = ddlToSparkSchema(tableName, ddl).json();

            lakesoulDBManager.createNewTable(tableId, dbName, tableName, qualifiedPath,
                                             tableSchema,
                                             new JSONObject(), ""
            );
        }
    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            System.out.printf("Namespace %s already exists%n", namespace);
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
                result=rs.getString("Create Table");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return result;
    }

    public StructType ddlToSparkSchema(String tableName, String ddl) {
        final StructType[] stNew = {new StructType()};

        parser.parse(ddl, new Tables());
        parser.databaseTables().forTable(null, null, tableName).columns()
            .forEach(col-> {
                String name = col.name();
                DataType datatype = converter.schemaBuilder(col);
                if (datatype == null) {
                    throw new IllegalStateException("Unhandled data types");
                }
                System.out.println(col);
                System.out.println(datatype.json());
                stNew[0] = stNew[0].add(name, datatype, col.isOptional());
            });

        return stNew[0];
    }
}
