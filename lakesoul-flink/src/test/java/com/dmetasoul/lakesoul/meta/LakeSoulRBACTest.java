// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.test.LakeSoulFlinkTestBase;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;

public class LakeSoulRBACTest extends LakeSoulFlinkTestBase {
    final String ADMIN1 = "admin1";
    final String ADMIN1_PASS = "admin1";
    final String ADMIN2 = "admin2";
    final String ADMIN2_PASS = "admin2";
    final String USER1 = "user1";
    final String USER1_PASS = "user1";
    final String USER2 = "user2";
    final String USER2_PASS = "user2";
    final String DOMAIN1 = "domain1";
    final String DOMAIN2 = "domain2";

    @AfterEach
    public void cleanMeta() throws Exception {
        resetMetaConn("lakesoul_test", "lakesoul_test", "public");
        dbManager.cleanMeta();
        LakeSoulFlinkTestBase.catalog = null;
    }

    private static void resetMetaConn(String username, String password, String domain) {
        System.setProperty(DBUtil.usernameKey, username);
        System.setProperty(DBUtil.passwordKey, password);
        System.setProperty(DBUtil.domainKey, domain);
        DBConnector.closeAllConnections();
        NativeMetadataJavaClient.closeAll();
    }

    private void login(String username, String password, String domain) {
        resetMetaConn(username, password, domain);
        System.setProperty("HADOOP_USER_NAME", username);
        try {
            UserGroupInformation.loginUserFromSubject(null);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDifferentDomain() {
        getTableEnv().useCatalog("lakesoul");
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);

        // create
        sql("create database if not exists database1");
        assert (sql("show databases").size() == 2);
        // create tables
        sql("use database1");
        sql("create table if not exists table1 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table1")
                + "')");
        sql("create table if not exists table2 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table2")
                + "')");
        assert (sql("show tables").size() == 2);

        // drop table
        sql("drop table table1");
        sql("drop table table2");
        assert (sql("show tables").size() == 0);

        // write and read data
        sql("create table if not exists table1 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table1")
                + "')");
        sql("insert into table1 values(1, 'foo1', 'bar1')");
        sql("insert into table1 values(2, 'foo2', 'bar2')");
        assert (sql("select * from table1").size() == 2);

        // create & drop database
        sql("insert into table1 values(3, 'foo3', 'bar3')");
        login(ADMIN2, ADMIN2_PASS, DOMAIN1);
        try {
            sql("use database1");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            assert (e instanceof CatalogException);
        }
        try {
            sql("create database if not exists database2");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getMessage().contains("Could not execute CREATE DATABASE");
        }

        // create table & drop table
        try {
            sql("create table if not exists database1.table3 ( id int, foo string, bar string )"
                    + " with ('format' = 'lakesoul', 'path' = '"
                    + getTempDirUri("/lakeSource/table3")
                    + "')");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getCause() instanceof DatabaseNotExistException;
        }
        try {
            sql("drop table database1.table1");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getMessage().contains("Table with identifier 'lakesoul.database1.table1' does not exist.");
        }

        // CRUD data
        try {
            sql("insert into database1.table1 values(4, 'foo4', 'bar4')");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert (e.getMessage().contains("Cannot find table '`lakesoul`.`database1`.`table1`' in any of the catalogs"));
        }

        try {
            sql("select * from database1.table1");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert (e.getMessage().contains("Object 'database1' not found within 'lakesoul'"));
        }

        // clear test
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        sql("drop database if exists database1 cascade");
    }

    @Test
    public void testDifferentRole() {
        getTableEnv().useCatalog("lakesoul");
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        // create
        sql("create database if not exists database1");


        login(USER1, USER1_PASS, DOMAIN1);
        // create table & drop database
        sql("use database1");
        try {
            sql("create database if not exists database3");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getMessage().contains("Could not execute CREATE DATABASE");
        }
        try {
            sql("drop database database1");
            throw new RuntimeException("test state was unexcepted");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            assert e.getMessage().contains("Could not execute DROP DATABASE lakesoul.database1 RESTRICT");
        }

        // create & drop table
        sql("create table if not exists table1 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table1")
                + "')");
        sql("create table if not exists table2 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table2")
                + "')");
        assert (sql("show tables").size() == 2);
        sql("drop table table1");
        sql("drop table table2");
        assert (sql("show tables").size() == 0);

        // CRUD data
        sql("create table if not exists table1 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + getTempDirUri("/lakeSource/table2")
                + "')");
        sql("insert into table1 values(1, 'foo1', 'bar1')");
        sql("insert into table1 values(2, 'foo2', 'bar2')");
        assert (sql("select * from table1").size() == 2);

        // clear test
        sql("drop table table1");
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        sql("drop database if exists database1 cascade");
        sql("drop database if exists database2 cascade");
        sql("drop database if exists database3 cascade");
    }

    // To run this test, please set HADOOP_HOME env var.
    @Test
    public void testHDFSDirPermission() {
        getTableEnv().useCatalog("lakesoul");
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        // create namespace
        sql("create database if not exists database1");
        login(USER1, USER1_PASS, DOMAIN1);
        // create table
        sql("use database1");
        Path tablePath = new Path("hdfs://localhost:9000/lakesoul-test-bucket/database1/table1");
        sql("create table if not exists table1 ( id int, foo string, bar string )"
                + " with ('format' = 'lakesoul', 'path' = '"
                + tablePath.toString()
                + "')");
        // table owner can read/write
        sql("insert into table1 values(1, 'foo1', 'bar1')");
        sql("select * from table1");
        login(USER2, USER2_PASS, DOMAIN1);
        // user in same domain can read
        sql("select * from table1");
        // user in same domain can't write
        Assert.assertThrows(RuntimeException.class, () -> sql("insert into table1 values(2, 'foo2', 'bar2')"));
        login(ADMIN1, ADMIN1_PASS, DOMAIN1);
        sql("drop database if exists database1 cascade");
    }
}
