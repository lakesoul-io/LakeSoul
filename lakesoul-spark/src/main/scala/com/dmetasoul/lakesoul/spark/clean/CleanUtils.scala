// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.clean

import com.dmetasoul.lakesoul.meta.DBConnector
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql.ResultSet
import java.util
import scala.collection.mutable.ArrayBuffer

object CleanUtils {

  private val conn = DBConnector.getConn

  def createStructField(name: String, colType: String): StructField = {
    colType match {
      case "java.lang.String" => StructField(name, StringType, nullable = true)
      case "java.lang.Integer" => StructField(name, IntegerType, nullable = true)
      case "java.lang.Long" => StructField(name, LongType, nullable = true)
      case "java.lang.Boolean" => StructField(name, BooleanType, nullable = true)
      case "java.lang.Double" => StructField(name, DoubleType, nullable = true)
      case "java.lang.Float" => StructField(name, FloatType, nullable = true)
      case "java.sql.Date" => StructField(name, DateType, nullable = true)
      case "java.sql.Time" => StructField(name, TimestampType, nullable = true)
      case "java.sql.Timestamp" => StructField(name, TimestampType, nullable = true)
      case "java.math.BigDecimal" => StructField(name, DecimalType(10, 0), nullable = true)

    }
  }

  /**
   * Convert the detected ResultSet into a DataFrame
   */
  def createResultSetToDF(rs: ResultSet, sparkSession: SparkSession): DataFrame = {
    val rsmd = rs.getMetaData
    val columnTypeList = new util.ArrayList[String]
    val rowSchemaList = new util.ArrayList[StructField]
    for (i <- 1 to rsmd.getColumnCount) {
      var temp = rsmd.getColumnClassName(i)
      temp = temp.substring(temp.lastIndexOf(".") + 1)
      if ("Integer".equals(temp)) {
        temp = "Int"
      }
      columnTypeList.add(temp)
      rowSchemaList.add(createStructField(rsmd.getColumnName(i), rsmd.getColumnClassName(i)))
    }
    val rowSchema = StructType(rowSchemaList)
    val rsClass = rs.getClass
    var count = 1
    val resultList = new util.ArrayList[Row]
    var totalDF = sparkSession.createDataFrame(new util.ArrayList[Row], rowSchema)
    while (rs.next()) {
      count = count + 1
      val buffer = new ArrayBuffer[Any]()
      for (i <- 0 until columnTypeList.size()) {
        val method = rsClass.getMethod("get" + columnTypeList.get(i), "aa".getClass)
        buffer += method.invoke(rs, rsmd.getColumnName(i + 1))
      }
      resultList.add(Row(buffer: _*))
      if (count % 100000 == 0) {
        val tempDF = sparkSession.createDataFrame(resultList, rowSchema)
        totalDF = totalDF.union(tempDF).distinct()
        resultList.clear()
      }
    }
    val tempDF = sparkSession.createDataFrame(resultList, rowSchema)
    totalDF = totalDF.union(tempDF)
    totalDF
  }

  def sqlToDataframe(sql: String, spark: SparkSession): DataFrame = {
    val stmt = conn.prepareStatement(sql)
    val resultSet = stmt.executeQuery()
    createResultSetToDF(resultSet, spark)
  }

  def setTableDataExpiredDays(tablePath: String, expiredDays: Int): Unit = {
    val sql =
      s"""
         |UPDATE table_info
         |SET properties = properties::jsonb || '{"partition.ttl": "$expiredDays"}'::jsonb
         |WHERE table_id = (SELECT table_id from table_info where table_path='$tablePath');
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def setCompactionExpiredDays(tablePath: String, expiredDays: Int): Unit = {
    val sql =
      s"""
         |UPDATE table_info
         |SET properties = properties::jsonb || '{"compaction.ttl": "$expiredDays"}'::jsonb
         |WHERE table_id = (SELECT table_id from table_info where table_path='$tablePath');
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def setTableOnlySaveOnceCompactionValue(tablePath: String, value: Boolean): Unit = {
    val sql =
      s"""
         |UPDATE table_info
         |SET properties = properties::jsonb || '{"only_save_once_compaction": "$value"}'::jsonb
         |WHERE table_id = (SELECT table_id from table_info where table_path='$tablePath');
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def cancelTableDataExpiredDays(tablePath: String): Unit = {
    val sql =
      s"""
         |UPDATE table_info
         |SET properties = properties::jsonb - 'partition.ttl'
         |WHERE table_id = (SELECT table_id from table_info where table_path='$tablePath');
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def cancelCompactionExpiredDays(tablePath: String): Unit = {
    val sql =
      s"""
         |UPDATE table_info
         |SET properties = properties::jsonb - 'compaction.ttl'
         |WHERE table_id = (SELECT table_id from table_info where table_path='$tablePath');
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def setPartitionInfoTimestamp(tableId: String, timestamp: Long, version: Int): Unit = {
    val sql =
      s"""
         |UPDATE partition_info
         |SET timestamp = $timestamp
         |WHERE table_id = '$tableId'
         |AND version = $version
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def readPartitionInfo(tableId: String, spark: SparkSession): DataFrame = {
    val sql =
      s"""
         |SELECT table_id,partition_desc,commit_op,version,timestamp
         |FROM partition_info
         |WHERE table_id = '$tableId'
         |""".stripMargin
    sqlToDataframe(sql, spark)
  }

  def readDataCommitInfo(tableId: String, spark: SparkSession): DataFrame = {
    val sql =
      s"""
         |SELECT table_id,partition_desc,commit_op
         |FROM data_commit_info
         |WHERE table_id = '$tableId'
         |""".stripMargin
    sqlToDataframe(sql, spark)
  }
}