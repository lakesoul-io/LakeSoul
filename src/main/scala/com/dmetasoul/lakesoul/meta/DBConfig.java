package com.dmetasoul.lakesoul.meta;

public abstract class DBConfig {

    static String driver = "org.postgresql.Driver";
    static String url = "jdbc:postgresql://127.0.0.1:5432/test_lakesoul_meta?stringtype=unspecified";
    static String username = "yugabyte";
    static String password = "yugabyte";

    static int MAX_COMMIT_ATTEMPTS = 5;
}
