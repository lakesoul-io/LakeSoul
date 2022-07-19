package com.dmetasoul.lakesoul.meta;

public abstract class DBConfig {
    //TODO
//    static String driver = "org.postgresql.Driver";
//    static String url = "jdbc:postgresql://172.31.252.175:5432/test_lakesoul_meta?stringtype=unspecified";
//    static String username = "admin";
//    static String password = "eH9hdJkqYORNsNN6OGjF";

    static String driver = "org.postgresql.Driver";
    static String url = "jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified";
    static String username = "yugabyte";
    static String password = "yugabyte";

    static int MAX_COMMIT_ATTEMPTS = 5;
}
