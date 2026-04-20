// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.benchmark;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

public class Benchmark {
    static String hostname = "mysql";
    //static String hostname = "localhost";
    static String  mysqlUserName = "root";
    static String  mysqlPassword = "root";
    static int mysqlPort = 3306;
    static String  serverTimeZone = "UTC";
    static String prestoHost = "127.0.0.1";
    static String prestoPort = "8080";


    /** for CDC */
    static String  dbName = "test_cdc";
    static boolean verifyCDC = true;

    /** for single test  */
    static boolean singleLakeSoulContrast = false;
    static String  lakeSoulDBName = "default";
    static String  lakeSoulTableName = "default_init";

    static final String  DEFAULT_INIT_TABLE = "default_init";
    static final String  printLine = " ******** ";
    static final String  splitLine = " --------------------------------------------------------------- ";

    static Connection mysqlCon = null;
    static Connection prestoCon = null;

    /**
     * param example:
     * --mysql.hostname localhost
     * --mysql.database.name default_init
     * --mysql.username root
     * --mysql.password root
     * --mysql.port 3306
     * --server.time.zone UTC
     * --cdc.contract true
     * --single.table.contract false
     * --lakesoul.database.name lakesoul_test
     * --lakesoul.table.name lakesoul_table
     */
    public static void main(String[] args) throws Exception {
        ParametersTool parameter = ParametersTool.fromArgs(args);
        hostname = parameter.get("mysql.hostname", hostname);
        dbName = parameter.get("mysql.database.name", dbName);
        mysqlUserName = parameter.get("mysql.username", mysqlUserName);
        mysqlPassword = parameter.get("mysql.password", mysqlPassword);
        mysqlPort = parameter.getInt("mysql.port", mysqlPort);
        prestoHost = parameter.get("presto.hostname", prestoHost);
        prestoPort = parameter.get("presto.port", prestoPort);
        serverTimeZone = parameter.get("server.time.zone", serverTimeZone);
        verifyCDC = parameter.getBoolean("cdc.contract", true);

        String mysqlUrl = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone;
        String prestoUrl = "jdbc:presto://" + prestoHost + ":" + prestoPort + "/lakesoul/";
        mysqlCon = DriverManager.getConnection(mysqlUrl, mysqlUserName, mysqlPassword);
        prestoCon = DriverManager.getConnection(prestoUrl, "test", null);

        singleLakeSoulContrast = parameter.getBoolean("single.table.contract", false);
        if (singleLakeSoulContrast) {
            lakeSoulDBName = parameter.get("lakesoul.database.name", lakeSoulDBName);
            lakeSoulTableName = parameter.get("lakesoul.table.name", lakeSoulTableName);
            mysqlCon.setSchema(lakeSoulDBName);
            prestoCon.setSchema(lakeSoulDBName);
            System.out.println(splitLine);
            System.out.println("verifing single tableName = " + lakeSoulDBName + "." + lakeSoulTableName);
            verifyQuery(dbName, lakeSoulDBName, lakeSoulTableName);
            System.out.println(splitLine);
        }

        if (verifyCDC) {
            mysqlCon.setSchema(dbName);
            prestoCon.setSchema(dbName);
            // query tables in mysql and then verify them in presto
            ResultSet tablesResults = mysqlCon.prepareStatement("show tables").executeQuery();
            while (tablesResults.next()){
                String tableName = tablesResults.getString(1);
                System.out.println(splitLine);
                System.out.println("verifing tableName = " + dbName + "." + tableName);
                verifyQuery(dbName, lakeSoulDBName, tableName);
            }
        }

        mysqlCon.close();
        prestoCon.close();
    }

    @Deprecated
    public static void verifyQueryHard (String table) throws SQLException {
        String sql = "select * from " + table;
        ResultSet res1 = mysqlCon.prepareStatement(sql).executeQuery();
        ResultSet res2 = prestoCon.prepareStatement(sql).executeQuery();
        while (res1.next()){
            if(!res2.next()) {
                throw new RuntimeException("row count do not match");
            }
            int n = res1.getMetaData().getColumnCount();
            for(int i = 1; i <= n; i++){
                Object object1 = res1.getObject(i);
                Object object2 = res2.getObject(i);
                if(object1 == object2 || object1.equals(object2)){
                    continue;
                }
                throw new RuntimeException("not equals: " + object1  + " and " + object2);
            }
        }

        System.out.println("table " + table + " matched");
    }


     public static void verifyQuery(String sourceSchema, String sinkSchema, String table) throws SQLException {
        
        String sourceSchemaTableName = table; 
        String sinkSchemaTableName =  sinkSchema + ".s_test_cdc_" + table;

        String prestoSqlCount = String.format("select count(*) from lakesoul.%s", sinkSchemaTableName);
        String mysqlSqlCount = String.format("select count(*) from %s", sourceSchemaTableName);

        String prestoSqlData = String.format("select * from lakesoul.%s", sinkSchemaTableName);
        String mysqlSqlData = String.format("select * from %s", sourceSchemaTableName);

        try {
            int countLakeSoul = getCount(prestoCon.prepareStatement(prestoSqlCount).executeQuery());
            int countMysql = getCount(mysqlCon.prepareStatement(mysqlSqlCount).executeQuery());
            if (countLakeSoul != countMysql) {
                System.out.println("lakesoul." + table + " count=" + countLakeSoul);
                System.out.println("mysql." + table + " count=" + countMysql);
                throw new RuntimeException("table " + table + " count is not matched!");
            }

            ResultSet rsPresto = prestoCon.prepareStatement(prestoSqlData).executeQuery();
            ResultSet rsMysql = mysqlCon.prepareStatement(mysqlSqlData).executeQuery();

            Set<String> prestoRows = extractRowsToSet(rsPresto);
            Set<String> mysqlRows = extractRowsToSet(rsMysql);

            // lakesoul except mysql
            Set<String> lakesoulExceptMysql = new HashSet<>(prestoRows);
            lakesoulExceptMysql.removeAll(mysqlRows);

            // mysql except lakesoul
            Set<String> mysqlExceptLakesoul = new HashSet<>(mysqlRows);
            mysqlExceptLakesoul.removeAll(prestoRows);

            if (!lakesoulExceptMysql.isEmpty() || !mysqlExceptLakesoul.isEmpty()) {
                System.out.println(table + " lakesoul - mysql diff count=" + lakesoulExceptMysql.size());
                System.out.println(table + " mysql - lakesoul diff count=" + mysqlExceptLakesoul.size());
                throw new RuntimeException("table " + table + " data content is not matched");
            }

            System.out.println("table " + table + " matched");

        } catch (Exception e) {
            System.out.println("table " + table + " not matched! Exception: " + e.getMessage());
            try {
                System.out.println("========= LakeSoul Data =========");
                ResultSetPrinter.printResultSet(prestoCon.prepareStatement(prestoSqlData).executeQuery());
                System.out.println("========= MySQL Data =========");
                ResultSetPrinter.printResultSet(mysqlCon.prepareStatement(mysqlSqlData).executeQuery());
            } catch (Exception ex) {
                System.out.println("Failed to print results: " + ex.getMessage());
            }
            throw new RuntimeException(e);
        }
    }

    private static Set<String> extractRowsToSet(ResultSet rs) throws SQLException {
        Set<String> rows = new HashSet<>();
        int columnCount = rs.getMetaData().getColumnCount();

        java.time.format.DateTimeFormatter dateTimeFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        java.time.format.DateTimeFormatter dateFormatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd");

        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                Object obj = rs.getObject(i);
                if (obj == null) {
                    sb.append("NULL|");
                    continue;
                }

                String val;
                if (obj instanceof byte[]) {
                    byte[] bytes = (byte[]) obj;
                    StringBuilder hex = new StringBuilder();
                    for (byte b : bytes) {
                        hex.append(String.format("%02x", b));
                    }
                    val = hex.toString();
                } else if (obj instanceof java.sql.Timestamp) {
                    val = ((java.sql.Timestamp) obj)
                            .toLocalDateTime()
                            .format(dateTimeFormatter);
                } else if (obj instanceof java.time.LocalDateTime) {
                    val = ((java.time.LocalDateTime) obj)
                            .format(dateTimeFormatter);
                } else if (obj instanceof java.sql.Date) {
                    val = ((java.sql.Date) obj)
                            .toLocalDate()
                            .format(dateFormatter);
                } else if (obj instanceof java.time.LocalDate) {
                    val = ((java.time.LocalDate) obj)
                            .format(dateFormatter);
                } else {
                    val = obj.toString().trim();
                }

                // 数字型字符串兼容处理，可留可不留
                if (val.endsWith(".0")) {
                    val = val.substring(0, val.length() - 2);
                }

                sb.append(val).append("|");
            }
            rows.add(sb.toString());
        }
        return rows;
    }

    static int getCount(ResultSet res) throws SQLException {
        if(res.next()){
            return res.getInt(1);
        }
        throw new RuntimeException("resultset has not records");
    }
}


/**
 * Result Printer - print resultset to table
 */
class ResultSetPrinter {
    public static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData resultSetMetaData = rs.getMetaData();
        // get ColumnCount
        int ColumnCount = resultSetMetaData.getColumnCount();
        // get col max length
        int[] columnMaxLengths = new int[ColumnCount];
        ArrayList<String[]> results = new ArrayList<>();
        while (rs.next()) {
            String[] columnStr = new String[ColumnCount];
            for (int i = 0; i < ColumnCount; i++) {
                Object obj = rs.getObject(i + 1);
                if (obj instanceof byte[]) {
                    byte[] bytes = (byte[]) obj;
                    StringBuilder hex = new StringBuilder();
                    for (int j = 0; j < Math.min(bytes.length, 10); j++) hex.append(String.format("%02x", bytes[j]));
                    columnStr[i] = hex.toString() + (bytes.length > 10 ? "..." : "");
                } else {
                    columnStr[i] = (obj == null) ? "NULL" : obj.toString();
                }
                columnMaxLengths[i] = Math.max(columnMaxLengths[i], (columnStr[i] == null) ? 0 : columnStr[i].length());
            }
            results.add(columnStr);
        }
        printSeparator(columnMaxLengths);
        printColumnName(resultSetMetaData, columnMaxLengths);
        printSeparator(columnMaxLengths);
        // output results
        Iterator<String[]> iterator = results.iterator();
        String[] columnStr;
        while (iterator.hasNext()) {
            columnStr = iterator.next();
            for (int i = 0; i < ColumnCount; i++) {
                System.out.printf("|%" + columnMaxLengths[i] + "s", columnStr[i]);
            }
            System.out.println("|");
        }
        printSeparator(columnMaxLengths);
    }

    /**
     * ouput column name.
     * @param resultSetMetaData ResultSet meta
     * @param columnMaxLengths column max length
     * @throws SQLException
     */
    private static void printColumnName(ResultSetMetaData resultSetMetaData, int[] columnMaxLengths) throws SQLException {
        int columnCount = resultSetMetaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            System.out.printf("|%" + columnMaxLengths[i] + "s", resultSetMetaData.getColumnName(i + 1));
        }
        System.out.println("|");
    }

    /**
     * print spliter.
     * @param columnMaxLengths column max length
     */
    private static void printSeparator(int[] columnMaxLengths) {
        for (int i = 0; i < columnMaxLengths.length; i++) {
            System.out.print("+");
            for (int j = 0; j < columnMaxLengths[i]; j++) {
                System.out.print("-");
            }
        }
        System.out.println("+");
    }

}

