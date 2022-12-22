package org.apache.flink.lakesoul.test;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import com.dmetasoul.lakesoul.meta.external.mysql.MysqlDBManager;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.apache.flink.lakesoul.tool.JobOptions.JOB_CHECKPOINT_INTERVAL;
import static org.apache.flink.lakesoul.tool.LakeSoulDDLSinkOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BUCKET_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SOURCE_PARALLELISM;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.WAREHOUSE_PATH;

public class OracleDBManagerTest {
    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        String dbName = parameter.get(SOURCE_DB_DB_NAME.key());
        String userName = parameter.get(SOURCE_DB_USER.key());
        String passWord = parameter.get(SOURCE_DB_PASSWORD.key());
        String host = parameter.get(SOURCE_DB_HOST.key());
        int port = parameter.getInt(SOURCE_DB_PORT.key(), MysqlDBManager.DEFAULT_MYSQL_PORT);
        String databasePrefixPath = parameter.get(WAREHOUSE_PATH.key());
        int sourceParallelism = parameter.getInt(SOURCE_PARALLELISM.key(), SOURCE_PARALLELISM.defaultValue());
        int bucketParallelism = parameter.getInt(BUCKET_PARALLELISM.key(), BUCKET_PARALLELISM.defaultValue());
        int checkpointInterval = parameter.getInt(JOB_CHECKPOINT_INTERVAL.key(),
                JOB_CHECKPOINT_INTERVAL.defaultValue());     //mill second

        OracleDBManager dbManager = new OracleDBManager(dbName,
                                                        userName,
                                                        passWord,
                                                        host,
                                                        Integer.toString(port),
                                                        new HashSet<>(),
                                                        new HashSet<>(),
                                                        databasePrefixPath,
                                                        bucketParallelism,
                                                        true);
        System.out.println(dbManager.checkOpenStatus());
        dbManager.listNamespace().forEach(System.out::println);
        dbManager.listTables().forEach(System.out::println);
        dbManager.importOrSyncLakeSoulTable("employees_demo");
    }
}

class OracleDBManager implements ExternalDBManager {

    public static final int DEFAULT_ORACLE_PORT = 1521;
    private final DBConnector dbConnector;


    private final DBManager lakesoulDBManager = new DBManager();
    private final String lakesoulTablePathPrefix;
    private final String dbName;
    private final int hashBucketNum;
    private final boolean useCdc;
    private HashSet<String> excludeTables;
    private HashSet<String> includeTables;

    private String[] filterTables = new String[]{};

    OracleDBManager(String dbName,
                    String user,
                    String passwd,
                    String host,
                    String port, HashSet<String> excludeTables,
                    HashSet<String> includeTables,
                    String pathPrefix, int hashBucketNum, boolean useCdc
              ) {
        this.dbName = dbName;
        this.excludeTables = excludeTables;
        this.includeTables = includeTables;
        excludeTables.addAll(Arrays.asList(filterTables));


        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("oracle.jdbc.driver.OracleDriver");
        String url = "jdbc:oracle:thin:@" + host + ":" + port + "/" + dbName;
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        lakesoulTablePathPrefix = pathPrefix;
        this.hashBucketNum = hashBucketNum;
        this.useCdc = useCdc;
    }

    public boolean checkOpenStatus() {
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

    @Override
    public List<String> listTables() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "SELECT table_name FROM user_tables";
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

    public List<String> listNamespace() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select NAME from v$database";
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String database = rs.getString("NAME");
                list.add(database);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return list;
    }

    public String showCreateTable(String tableName) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("select table_name,dbms_metadata.get_ddl('TABLE','%s') as DDL from dual,user_tables where table_name='%s'", tableName.toUpperCase(), tableName.toUpperCase());
        String result = null;
        try {
            conn = dbConnector.getConn();
            System.out.println(sql);
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("DDL");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            dbConnector.closeConn(rs, pstmt, conn);
        }
        return result;
    }

    @Override
    public void importOrSyncLakeSoulTable(String tableName) {
        if (!includeTables.contains(tableName) && excludeTables.contains(tableName)) {
            System.out.println(String.format("Table %s is excluded by exclude table list", tableName));
            return;
        }
        String ddl = showCreateTable(tableName);
        System.out.println(ddl);
    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {

    }
}
