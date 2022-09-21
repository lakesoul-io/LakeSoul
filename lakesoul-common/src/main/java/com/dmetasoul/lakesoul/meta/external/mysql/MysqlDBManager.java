package com.dmetasoul.lakesoul.meta.external.mysql;

import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.DatabaseSchemaedTables;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class MysqlDBManager implements ExternalDBManager {

    private DBConnector dbConnector;
    private String defaultDBName;
    private HashSet<String> excludeTables;
    private String[] filterTables = new String[]{"sys_config"};

    public MysqlDBManager(String DBName, String user, String passwd, String host, String port, HashSet<String> excludeTables) {
        this.defaultDBName = DBName;
        this.excludeTables = excludeTables;
        excludeTables.addAll(Arrays.asList(filterTables));

        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + DBName + "?useSSL=false";
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);
    }


    public List<String> listTables() {
        return listTablesByNamespace(defaultDBName);
    }

    @Override
    public List<String> listTablesByNamespace(String namespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("show tables in %s", namespace);
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString(String.format("Tables_in_%s", namespace));
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
    public List<Namespace> listNamespaces() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "show databases";
        List<Namespace> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                Namespace namespace = new Namespace(rs.getString("Database"));
                list.add(namespace);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    @Override
    public DatabaseSchemaedTables getDatabaseAndTablesWithSchema() {
        return getDatabaseAndTablesWithSchema(defaultDBName);
    }

    public DatabaseSchemaedTables getDatabaseAndTablesWithSchema(String DBName) {
        Connection connection = null;
        DatabaseSchemaedTables dct = new DatabaseSchemaedTables(DBName);
        try {
            connection = dbConnector.getConn();
            DatabaseMetaData dmd = connection.getMetaData();

            ResultSet tables = dmd.getTables(DBName, null, null, new String[]{"TABLE"});
            while (tables.next()) {
                String tablename = tables.getString("TABLE_NAME");
                System.out.println(tablename);
                if (excludeTables.contains(tablename)) {
                    continue;
                }
                DatabaseSchemaedTables.Table tbl = dct.addTable(tablename);
                ResultSet cols = dmd.getColumns(null, null, tablename, null);
                while (cols.next()) {
//                    System.out.println(cols.getString("COLUMN_NAME")+" "+cols.getString("TYPE_NAME")+" "+cols.getString("COLUMN_SIZE"));
                    tbl.addColumn(cols.getString("COLUMN_NAME"), cols.getString("TYPE_NAME"));
                }
                ResultSet pks = dmd.getPrimaryKeys(null, null, tablename);
                while (pks.next()) {
                    tbl.addPrimaryKey(pks.getString("COLUMN_NAME"), pks.getShort("KEY_SEQ"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(connection);
        }

        return dct;
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
}
