// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnector {
    private final HikariConfig config = new HikariConfig();
    private HikariDataSource ds;
    private HikariDataSource standByDS;

    private static DBConnector instance = null;

    private void createDataSource() {
        DataBaseProperty dataBaseProperty = DBUtil.getDBInfo();
        try {
            config.setDriverClassName(dataBaseProperty.getDriver());
            config.setJdbcUrl(dataBaseProperty.getUrl());
            config.setUsername(dataBaseProperty.getUsername());
            config.setPassword(dataBaseProperty.getPassword());
            DBUtil.fillDataSourceConfig(config);
            ds = new HikariDataSource( config );
            if (!dataBaseProperty.getStandByUrl().equals(dataBaseProperty.getUrl())) {
                // specified standby url, create an extra ds
                config.setJdbcUrl(dataBaseProperty.getStandByUrl());
                standByDS = new HikariDataSource( config );
            }
        } catch (Throwable t) {
            System.err.println("Failed to connect to PostgreSQL Server with configs: " +
                    "driver=" + dataBaseProperty.getDriver() +
                    "; url=" + dataBaseProperty.getUrl() +
                    " ; user=" + config.getUsername());
            System.err.println("Please verify your meta connection configs according to doc: " +
                    "https://lakesoul-io.github.io/docs/Getting%20Started/setup-local-env");
            t.printStackTrace();
            throw t;
        }
    }

    private DBConnector() {}

    public static synchronized DataSource getDS() {
        if (instance == null) {
            instance = new DBConnector();
            instance.createDataSource();
        }
        return instance.ds;
    }

    public static synchronized Connection getConn() throws SQLException {
        if (instance == null) {
            instance = new DBConnector();
            instance.createDataSource();
        }
        return instance.ds.getConnection();
    }

    public static synchronized Connection getStandByConn() throws SQLException {
        if (instance == null) {
            instance = new DBConnector();
            instance.createDataSource();
        }
        if (instance.standByDS == null) {
            return instance.ds.getConnection();
        } else {
            return instance.standByDS.getConnection();
        }
    }

    public static synchronized void closeAllConnections()  {
        if (instance != null) {
            instance.ds.close();
            if (instance.standByDS != null) {
                instance.standByDS.close();
            }
            instance = null;
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(DBConnector::closeAllConnections));
    }

    public static void closeConn(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeConn(Statement statement, Connection conn) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        closeConn(conn);
    }

    public static void closeConn(ResultSet set, Statement statement, Connection conn) {
        if (set != null) {
            try {
                set.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        closeConn(statement, conn);
    }
}
