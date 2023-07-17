// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta.external;

import com.dmetasoul.lakesoul.meta.DataBaseProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBConnector {
    private final HikariDataSource ds;

    public DBConnector(DataBaseProperty dataBaseProperty) {
        HikariConfig config = new HikariConfig();

        config.setDriverClassName( dataBaseProperty.getDriver());
        config.setJdbcUrl( dataBaseProperty.getUrl());
        config.setUsername( dataBaseProperty.getUsername());
        config.setPassword( dataBaseProperty.getPassword());
        config.addDataSourceProperty( "cachePrepStmts" , "true" );
        config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
        config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
        ds = new HikariDataSource( config );
    }
    public  Connection getConn() throws SQLException {
        return ds.getConnection();
    }
    public  void closeConn()  {
        if(ds != null) {
            ds.close();
        }
    }
    public  void closeConn(Connection conn) {
        if (conn != null) {
           try {
               conn.close();
           } catch (SQLException e) {
               e.printStackTrace();
           }
        }
    }

    public  void closeConn(Statement statement, Connection conn) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        closeConn(conn);
    }

    public  void closeConn(ResultSet set, Statement statement, Connection conn) {
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
