// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.dmetasoul.lakesoul.meta.rbac.AuthZEnforcer
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, SparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.util.Locale

@RunWith(classOf[JUnitRunner])
class RBACOperatinSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulSQLCommandTest {

  final val ADMIN1: String = "admin1"
  final val ADMIN1_PASS: String = "admin1"
  final val ADMIN2: String = "admin2"
  final val ADMIN2_PASS: String = "admin2"
  final val USER1: String = "user1"
  final val USER1_PASS: String = "user1"
  final val DOMAIN1: String = "domain1"
  final val DOMAIN2: String = "domain2"

  def login(username: String, password: String, domain: String): Unit = {
    System.setProperty(DBUtil.usernameKey, username)
    System.setProperty(DBUtil.passwordKey, password)
    System.setProperty(DBUtil.domainKey, domain)
    DBConnector.closeAllConnections()

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

    // delete data
    spark.sql("delete from table1")
    val df6 = spark.sql("select * from table1").toDF()
    assert(df6.count() == 0)

  }

  test("testDifferentRole") {
//    login(ADMIN1, ADMIN1_PASS, DOMAIN1)
//    spark.sql("create database database1")
//    val df = spark.sql("show databases").toDF()
//    df.show()
//    df.show()

  }
}
