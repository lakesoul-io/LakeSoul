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

import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.*;

public class DBConnector {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    static {
        DataBaseProperty dataBaseProperty = DBUtil.getDBInfo();
        config.setDriverClassName( dataBaseProperty.getDriver());
        config.setJdbcUrl( dataBaseProperty.getUrl());
        config.setUsername( dataBaseProperty.getUsername());
        config.setPassword( dataBaseProperty.getPassword());
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }
    private DBConnector() {}
    public static Connection getConn() throws SQLException {
        return ds.getConnection();
    }
    public static void closeConn()  {
        if(ds != null) {
            ds.close();
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
