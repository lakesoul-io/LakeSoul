// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.external.mysql;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class MysqlDBManager implements ExternalDBManager {

    public static final int DEFAULT_MYSQL_PORT = 3306;
    private static final String EXTERNAL_MYSQL_TABLE_PREFIX = "external_mysql_table_";
    private final DBConnector dbConnector;

    private final DBManager lakesoulDBManager = new DBManager();
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private final int hashBucketNum;
    private final boolean useCdc;
    MysqlDataTypeConverter converter = new MysqlDataTypeConverter();
    private final HashSet<String> excludeTables;
    private final HashSet<String> includeTables;
    private final String[] filterTables = new String[]{"sys_config"};

    public MysqlDBManager(String dbName, String user, String passwd, String host, String port,
                          HashSet<String> excludeTables, String pathPrefix, int hashBucketNum, boolean useCdc) {
        this(dbName, user, passwd, host, port, excludeTables, new HashSet<>(), pathPrefix, hashBucketNum, useCdc);
    }

    public MysqlDBManager(String dbName, String user, String passwd, String host, String port,
                          HashSet<String> excludeTables, HashSet<String> includeTables, String pathPrefix,
                          int hashBucketNum, boolean useCdc) {
        this.dbName = dbName;
        this.excludeTables = excludeTables;
        this.includeTables = includeTables;
        excludeTables.addAll(Arrays.asList(filterTables));

        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("com.mysql.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?useSSL=false&allowPublicKeyRetrieval=true";
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        lakesoulTablePathPrefix = pathPrefix;
        this.hashBucketNum = hashBucketNum;
        this.useCdc = useCdc;
    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            return;
        }
        lakesoulDBManager.createNewNamespace(namespace, new JSONObject().toJSONString(), "");
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

}
