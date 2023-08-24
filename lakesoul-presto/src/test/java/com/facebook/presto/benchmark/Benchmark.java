package com.facebook.presto.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Benchmark {
    static String hostname = "localhost";
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
    static String  lakeSoulDBName = "flink_sink";
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
            verifyQuery(lakeSoulTableName);
            System.out.println(splitLine);
        }

        if (verifyCDC) {
            mysqlCon.setSchema(dbName);
            prestoCon.setSchema(dbName);
            ResultSet tablesResults = mysqlCon.prepareStatement("show tables").executeQuery();
            while (tablesResults.next()){
                verifyQuery(tablesResults.getString(1));
            }
        }

        mysqlCon.close();
        prestoCon.close();
    }

    static void verifyQuery (String table) throws SQLException {
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

//        if (table.equals(DEFAULT_INIT_TABLE)) {
//            jdbcDF = changeDF(jdbcDF)
//            lakesoulDF = changeDF(lakesoulDF)
//        }

    }

//    DataFrame changeDF(df: DataFrame) {
//        df.withColumn("col_2", col("col_2").cast("string"))
//                .withColumn("col_3", col("col_3").cast("string"))
//                .withColumn("col_11", col("col_11").cast("string"))
//                .withColumn("col_13", col("col_13").cast("string"))
//                .withColumn("col_20", col("col_20").cast("string"))
//                .withColumn("col_23", col("col_23").cast("string"))
//    }

}




