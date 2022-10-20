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

package org.apache.flink.lakesoul.source;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;

public class MysqlMetaDataSource implements JdbcMetaDataSource {

    HashSet<String> excludeTables;
    private final HikariConfig config = new HikariConfig();
    private final HikariDataSource ds;
    private final String databaseName;
    private final String[] filterTables = new String[]{"sys_config"};

    public MysqlMetaDataSource(String DBName, String user, String passwd, String host, String port,
                               HashSet<String> excludeTables) {
        this.excludeTables = excludeTables;
        excludeTables.addAll(Arrays.asList(filterTables));
        this.databaseName = DBName;
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + DBName + "?useSSL=false";
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(passwd);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(this.config);
    }

    @Override
    public DatabaseSchemaedTables getDatabaseAndTablesWithSchema() {
        Connection connection;
        DatabaseSchemaedTables dct = new DatabaseSchemaedTables(this.databaseName);
        try {
            connection = ds.getConnection();
            DatabaseMetaData dmd = connection.getMetaData();
            ResultSet tables = dmd.getTables(null, null, null, new String[]{"TABLE"});
            while (tables.next()) {
                String tablename = tables.getString("TABLE_NAME");
                if (excludeTables.contains(tablename)) {
                    continue;
                }
                DatabaseSchemaedTables.Table tbl = dct.addTable(tablename);
                ResultSet cols = dmd.getColumns(null, null, tablename, null);
                while (cols.next()) {
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
            ds.close();
        }

        return dct;
    }
}
