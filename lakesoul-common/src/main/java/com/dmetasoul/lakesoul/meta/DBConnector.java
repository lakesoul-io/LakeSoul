/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.meta;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnector {
    private final HikariConfig config = new HikariConfig();
    private HikariDataSource ds;

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
        } catch (Throwable t) {
            System.err.println("Failed to connect to PostgreSQL Server with configs: " +
                    "driver=" + dataBaseProperty.getDriver() +
                    "; url=" + dataBaseProperty.getUrl());
            t.printStackTrace();
            throw t;
        }
    }

    private DBConnector() {}

    public static synchronized Connection getConn() throws SQLException {
        if (instance == null) {
            instance = new DBConnector();
            instance.createDataSource();
        }
        return instance.ds.getConnection();
    }

    public static synchronized void closeAllConnections()  {
        if (instance != null) {
            instance.ds.close();
        }
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
