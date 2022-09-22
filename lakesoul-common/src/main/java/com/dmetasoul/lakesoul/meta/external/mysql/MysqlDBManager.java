package com.dmetasoul.lakesoul.meta.external.mysql;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.entity.DataBaseProperty;
import com.dmetasoul.lakesoul.meta.entity.Namespace;
import com.dmetasoul.lakesoul.meta.external.DBConnector;
import com.dmetasoul.lakesoul.meta.external.DatabaseSchemaedTables;
import com.dmetasoul.lakesoul.meta.external.ExternalDBManager;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Tables;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.*;


public class MysqlDBManager implements ExternalDBManager {

    private static final String TABLE_ID_PREFIX = "external_mysql_table_";

    private static final String TABLE_NAME_PREFIX = "external_mysql_table_";
    private DBConnector dbConnector;

    private DBManager lakesoulDBManager;
    private String DBName;
    private HashSet<String> excludeTables;
    private String[] filterTables = new String[]{"sys_config"};

    MysqlDataTypeConverter converter;

    MySqlAntlrDdlParser parser;

    public MysqlDBManager(String DBName, String user, String passwd, String host, String port, HashSet<String> excludeTables) {
        this.DBName = DBName;
        this.excludeTables = excludeTables;
        excludeTables.addAll(Arrays.asList(filterTables));

        DataBaseProperty dataBaseProperty = new DataBaseProperty();
        dataBaseProperty.setDriver("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + DBName + "?useSSL=false";
        dataBaseProperty.setUrl(url);
        dataBaseProperty.setUsername(user);
        dataBaseProperty.setPassword(passwd);
        dbConnector = new DBConnector(dataBaseProperty);

        lakesoulDBManager = new DBManager();

        converter = new MysqlDataTypeConverter();

        parser = new MySqlAntlrDdlParser(false,
                false,
                false,
                new MySqlValueConverters(
                        JdbcValueConverters.DecimalMode.DOUBLE,
                        TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS,
                        JdbcValueConverters.BigIntUnsignedMode.PRECISE,
                        CommonConnectorConfig.BinaryHandlingMode.BYTES),
                Tables.TableFilter.includeAll());
    }


    @Override
    public List<String> listTables() {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = String.format("show tables");
        List<String> list = new ArrayList<>();
        try {
            conn = dbConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString(String.format("Tables_in_%s", DBName));
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
    public void registerLakeSoulTable(String tableName) {

        String tableId = TABLE_ID_PREFIX + UUID.randomUUID();
        System.out.println(tableId);
        String namespace = DBName;
        String qualifiedPath = "";

        String ddl = showCreateTable(tableName);
        String tableSchema = ddlToSparkSchema(tableName, ddl).json();

        lakesoulDBManager.createNewTable(tableId, namespace, TABLE_NAME_PREFIX + tableName, qualifiedPath,
                tableSchema,
                new JSONObject(), ""
                );
    }

    @Override
    public void registerLakeSoulNamespace(String namespace) {
        if (lakesoulDBManager.getNamespaceByNamespace(namespace) != null) {
            System.out.println(String.format("Namespace %s already exists", namespace));
            return;
        }
        lakesoulDBManager.createNewNamespace(namespace, new JSONObject(), "");
    }

    @Override
    public DatabaseSchemaedTables getDatabaseAndTablesWithSchema() {
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

    public StructType ddlToSparkSchema(String tableName, String ddl) {
        final StructType[] stNew = {new StructType()};

        parser.parse(ddl, new Tables());
        parser.databaseTables().forTable(null, null, tableName).columns()
            .forEach(col-> {
                String name = col.name();
                DataType datatype = converter.schemaBuilder(col);
                System.out.println(name);
                System.out.println(datatype.json());
                stNew[0] = stNew[0].add(name, datatype, col.isOptional());
            });

        return stNew[0];
    }
}
