package org.apache.flink.lakesoul.test;

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

        OracleDBManager dbManager = new OracleDBManager(dbName, userName, passWord, host, Integer.toString(port));
        dbManager.listTables();
        System.out.println(dbManager.isOpen());
    }
}

class OracleDBManager implements ExternalDBManager {
    private final String dbName;

    public static final int DEFAULT_ORACLE_PORT = 1521;
    private final DBConnector dbConnector;

    OracleDBManager(String dbName,
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

    @Override
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
                String tableName = rs.getString(String.format("Tables_in_%s", dbName));
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

    }

    @Override
    public void importOrSyncLakeSoulNamespace(String namespace) {

    }
}
