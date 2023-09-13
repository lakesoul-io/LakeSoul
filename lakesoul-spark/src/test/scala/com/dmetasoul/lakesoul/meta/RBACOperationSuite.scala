// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.SparkException
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.Assert
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RBACOperationSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulSQLCommandTest {

  final val ADMIN1: String = "admin1"
  final val ADMIN1_PASS: String = "admin1"
  final val ADMIN2: String = "admin2"
  final val ADMIN2_PASS: String = "admin2"
  final val USER1: String = "user1"
  final val USER1_PASS: String = "user1"
  final val USER2: String = "user2"
  final val USER2_PASS: String = "user2"
  final val DOMAIN1: String = "domain1"
  final val DOMAIN2: String = "domain2"

  def resetMetaConn(username: String, password: String, domain: String): Unit = {
    println("TEST: LOGIN USERNAME " + username)
    println("TEST: LOGIN PASSWORD " + password)
    println("TEST: LOGIN DOMAIN " + domain)
    System.setProperty(DBUtil.usernameKey, username)
    System.setProperty(DBUtil.passwordKey, password)
    System.setProperty(DBUtil.domainKey, domain)
    DBConnector.closeAllConnections
    NativeMetadataJavaClient.closeAll
  }

  def login(username: String, password: String, domain: String): Unit = {
    resetMetaConn(username, password, domain)
    System.setProperty("HADOOP_USER_NAME", username)
    try UserGroupInformation.loginUserFromSubject(null)
    catch {
      case e: Exception =>
        e.printStackTrace()
        throw new RuntimeException(e)
    }
  }

  override def afterEach(): Unit = {
    resetMetaConn("lakesoul_test", "lakesoul_test", "public")
    val m = new DBManager()
    m.cleanMeta()
    super.afterEach()
  }

  test("testDifferentDomain") {
    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
    // create
    spark.sql("create database if not exists database1")
    val df = spark.sql("show databases").toDF()
    assert(df.count() == 2)
    // drop: coming soon
    //    spark.sql("drop database database1").collect()
    //    val df2 = spark.sql("show databases").toDF()
    //    assert(df2.count() == 1)
    //    assert(df2.collectAsList().get(0).getString(1).equals("default"))
    // create tables
    spark.sql("use database1;")
    spark.sql("create table if not exists table1 ( id int, foo string, bar string ) using lakesoul ")
    spark.sql("create table if not exists table2 ( id int, foo string, bar string ) using lakesoul ")
    val df3 = spark.sql("show tables").toDF()
    assert(df3.count() == 2)

    // drop table
    spark.sql("drop table table1")
    spark.sql("drop table table2")
    val df4 = spark.sql("show tables").toDF()
    assert(df4.count() == 0)

    // write and read data
    spark.sql("create table if not exists table1 ( id int, foo string, bar string ) using lakesoul ")
    spark.sql("insert into table1 values(1, 'foo1', 'bar1')")
    spark.sql("insert into table1 values(2, 'foo2', 'bar2')")
    val df5 = spark.sql("select * from table1").toDF()
    assert(df5.count() == 2)

    // update data
    spark.sql("update table1 set foo = 'foo3', bar = 'bar3'  where id = 2")
    val df6 = spark.sql("select (id, foo, bar) from table1 where id = 2").toDF()
    val row = df6.collectAsList().get(0).get(0).asInstanceOf[GenericRowWithSchema];
    assert(row.getString(1).equals("foo3"))
    assert(row.getString(2).equals("bar3"))

    // delete data
    spark.sql("delete from table1")
    val df7 = spark.sql("select * from table1").toDF()
    assert(df7.count() == 0)



    // create & drop database
    spark.sql("insert into table1 values(3, 'foo3', 'bar3')")
    login(ADMIN2, ADMIN2_PASS, DOMAIN1)
    val err0 = intercept[Exception] {
      spark.sql("use database1;")
    }
    assert(err0.isInstanceOf[NoSuchNamespaceException])
    val err1 = intercept[Exception] {
      spark.sql("create database if not exists database2")
    }
    println(err1.getMessage)
    assert(err1.getMessage.contains("new row violates row-level security policy for table \"namespace\""))

    val err11 = intercept[Exception] {
      spark.sql("drop database database1").collect()
    }
    println(err11.getMessage)
    assert(err11.isInstanceOf[NoSuchNamespaceException])

    // create table & drop table
    val err2 = intercept[Exception] {
      spark.sql("create table if not exists database1.table3 ( id int, foo string, bar string ) using lakesoul ")
    }
    println(err2.getMessage)
    assert(err2.isInstanceOf[NoSuchNamespaceException])
    val err3 = intercept[Exception] {
      spark.sql("drop table database1.table1")
    }
    println(err3.getMessage)
    assert(err3.getMessage.contains("Table or view not found"))

    // CRUD data
    val err4 = intercept[Exception] {
      spark.sql("insert into database1.table1 values(4, 'foo4', 'bar4')")
    }
    println(err4.getMessage)
    assert(err4.getMessage.contains("Table not found"))
    val err5 = intercept[Exception] {
      spark.sql("update database1.table1 set foo='foo4', bar='bar44' where id = 3")
    }
    println(err5.getMessage)
    assert(err5.getMessage.contains("Table or view not found"))

    val err6 = intercept[Exception] {
      spark.sql("select * from database1.table1")
    }
    println(err6.getMessage)
    assert(err6.getMessage.contains("Table or view not found"))

    val err7 = intercept[Exception] {
      spark.sql("delete from database1.table1 where id = 3")
    }
    println(err7.getMessage)
    assert(err7.getMessage.contains("Table or view not found"))

    // clear test
    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
    spark.sql("drop table table1")
  }

  test("testDifferentRole") {
    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
    // create
    spark.sql("create database if not exists database1")


    login(USER1, USER1_PASS, DOMAIN1)
    // create table & drop database
    spark.sql("use database1;")
    val err1 = intercept[Exception] {
      spark.sql("create database if not exists database3")
    }
    println(err1.getMessage)
    assert(err1.getMessage.contains("permission denied for table namespace"))

    val err2 = intercept[Exception] {
      spark.sql("drop database database1").collect()
    }
    println(err2.getMessage)
    assert(err2.getMessage.contains("permission denied for table namespace"))

    assert(spark.sql("show databases").toDF().count() == 2)

    // create & drop table
    spark.sql("create table if not exists table1 ( id int, foo string, bar string ) using lakesoul ")
    spark.sql("create table if not exists table2 ( id int, foo string, bar string ) using lakesoul ")
    assert(spark.sql("show tables").toDF().count() == 2)
    spark.sql("drop table table1")
    spark.sql("drop table table2")
    assert(spark.sql("show tables").toDF().count() == 0)

    // CRUD data
    spark.sql("create table if not exists table1 ( id int, foo string, bar string ) using lakesoul ")
    spark.sql("insert into table1 values(1, 'foo1', 'bar1')")
    spark.sql("insert into table1 values(2, 'foo2', 'bar2')")
    assert(spark.sql("select * from table1").toDF().count() == 2)
    spark.sql("update table1 set foo = 'foo3', bar = 'bar3'  where id = 2")
    val df1 = spark.sql("select (id, foo, bar) from table1 where id = 2").toDF()
    val row = df1.collectAsList().get(0).get(0).asInstanceOf[GenericRowWithSchema];
    assert(row.getString(1).equals("foo3"))
    assert(row.getString(2).equals("bar3"))
    spark.sql("delete from table1")
    val df7 = spark.sql("select * from table1").toDF()
    assert(df7.count() == 0)

    // clear test
    spark.sql("drop table table1")
  }

  // To run this test, please set HADOOP_HOME env var.
  test("test hdfs directory owner") {
    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
    // create namespace
    sql("create database if not exists database1")
    login(USER1, USER1_PASS, DOMAIN1)
    // create table
    sql("use database1")
    val tablePath = new Path("hdfs://localhost:9000/lakesoul-test-bucket/database1/table1")
    sql("create table if not exists table1 ( id int, foo string, bar string ) using lakesoul location '" + tablePath.toString + "'")
    // table owner can read/write// table owner can read/write
    sql("insert into table1 values(1, 'foo1', 'bar1')")
    sql("select * from table1")
    login(USER2, USER2_PASS, DOMAIN1)
    // user in same domain can read
    sql("select * from table1")
    // user in same domain can't write
    Assert.assertThrows(classOf[SparkException], () => sql("insert into table1 values(2, 'foo2', 'bar2')"))
    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
    sql("drop database if exists database1 cascade")
  }
}
