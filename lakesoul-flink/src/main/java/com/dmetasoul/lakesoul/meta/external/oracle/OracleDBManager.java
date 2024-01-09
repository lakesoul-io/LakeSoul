// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.external.oracle;

import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;

import java.io.IOException;
import java.util.List;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
public class OracleDBManager implements ExternalDBManager {
    private final String dbName;

    private final DBManager lakesoulDBManager = new DBManager();

    private final HashSet<String> includeTables = null;

    public static final int DEFAULT_ORACLE_PORT = 1521;
    private final DBConnector dbConnector;

    public OracleDBManager(String dbName,
                           String user,
                           String passwd,
                           String host,
                           String port
    ) {
        this.dbName = dbName;


        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName;
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

    }

    public OracleDBManager(String dbName, DBConnector dbConnector) {
        this.dbName = dbName;
        this.dbConnector = dbConnector;
    }

    public boolean isOpen() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select status from v$instance";
        boolean opened = false;
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                opened = rs.getString("STATUS").equals("OPEN");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return opened;
    }

    public List<String> listTables() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT table_name FROM user_tables ORDER BY table_name";
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString("table_name");
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
    public void importOrSyncLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null){
            return;
        }
        lakesoulDBManager.createNewNamespace(namespace,new JSONObject().toJSONString(),"");
    }

    public String showCreateTable(String tableName) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select dbms_metadata.get_ddl('TABLE','%s') from dual", tableName);
        String result = null;
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                result = rs.getString(String.format("DBMS_METADATA.GET_DDL('TABLE','%s')",tableName));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return result;
    }
}
