// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.alibaba.fastjson.JSONObject

import java.util
import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object MetaVersion {

  val dbManager = new DBManager()

  def createNamespace(namespace: String): Unit = {
    dbManager.createNewNamespace(namespace, new JSONObject().toJSONString, "")
  }

  def listNamespaces(): Array[String] = {
    dbManager.listNamespaces.asScala.toArray
  }

  def isTableExists(table_name: String): Boolean = {
    dbManager.isTableExists(table_name)
  }

  def isTableIdExists(table_name: String, table_id: String): Boolean = {
    dbManager.isTableIdExists(table_name, table_id)
  }

  def isNamespaceExists(table_namespace: String): Boolean = {
    dbManager.isNamespaceExists(table_namespace)
  }

  //  //check whether short_table_name exists, and return table path if exists
  //  def isShortTableNameExists(short_table_name: String): (Boolean, String) =
  //    isShortTableNameExists(short_table_name, LakeSoulCatalog.showCurrentNamespace().mkString("."))

  //check whether short_table_name exists, and return table path if exists
  def isShortTableNameExists(short_table_name: String, table_namespace: String): (Boolean, String) = {
    val path = dbManager.getTablePathFromShortTableName(short_table_name, table_namespace)
    if (path == null) (false, null) else (true, path)
  }

  //  def getTablePathFromShortTableName(short_table_name: String): String =
  //    getTablePathFromShortTableName(short_table_name, LakeSoulCatalog.showCurrentNamespace().mkString("."))

  //get table path, if not exists, return "not found"
  def getTablePathFromShortTableName(short_table_name: String, table_namespace: String): String = {
    dbManager.getTablePathFromShortTableName(short_table_name, table_namespace)
  }

  def createNewTable(table_namespace: String,
                     table_path: String,
                     short_table_name: String,
                     table_id: String,
                     table_schema: String,
                     range_column: String,
                     hash_column: String,
                     configuration: Map[String, String],
                     bucket_num: Int): Unit = {

    val partitions = DBUtil.formatTableInfoPartitionsField(hash_column, range_column)
    val json = new JSONObject()
    configuration.foreach(x => json.put(x._1, x._2))
    json.put("hashBucketNum", String.valueOf(bucket_num))
    dbManager.createNewTable(table_id, table_namespace, short_table_name, table_path, table_schema, json, partitions)
  }

  //  def listTables(): util.List[String] = {
  //    listTables(LakeSoulCatalog.showCurrentNamespace())
  //  }

  def listTables(namespace: Array[String]): util.List[String] = {
    dbManager.listTablePathsByNamespace(namespace.mkString("."))
  }

  //  def getTableInfo(table_path: String): TableInfo = {
  //    getTableInfo(LakeSoulCatalog.showCurrentNamespace().mkString("."), table_path)
  //  }

  //  def getTableInfo(namespace: String, table_path: String): TableInfo = {
  //    val info = dbManager.getTableInfoByPath(table_path)
  //    if (info == null) {
  //      return null
  //    }
  //    val short_table_name = info.getTableName
  //    val partitions = info.getPartitions
  //    val properties = info.getProperties.toString()
  //
  //    import scala.util.parsing.json.JSON
  //    val configuration = JSON.parseFull(properties)
  //    val configurationMap = configuration match {
  //      case Some(map: collection.immutable.Map[String, String]) => map
  //    }
  //
  //    // table may have no partition at all or only have range or hash partition
  //    val partitionCols = Splitter.on(';').split(partitions).asScala.toArray
  //    val (range_column, hash_column) = partitionCols match {
  //      case Array(range, hash) => (range, hash)
  //      case _ => ("", "")
  //    }
  //    val bucket_num = configurationMap.get("hashBucketNum") match {
  //      case Some(value) => value.toInt
  //      case _ => -1
  //    }
  //    TableInfo(
  //      namespace,
  //      Some(table_path),
  //      info.getTableId,
  //      info.getTableSchema,
  //      range_column,
  //      hash_column,
  //      bucket_num,
  //      configurationMap,
  //      if (short_table_name.equals("")) None else Some(short_table_name)
  //    )
  //  }

  def getSinglePartitionInfo(table_id: String, range_value: String, range_id: String): PartitionInfo = {
    val info = dbManager.getSinglePartitionInfo(table_id, range_value)
    PartitionInfo(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshotList.asScala.map(str => UUID.fromString(str)).toArray,
      expression = info.getExpression,
      commit_op = info.getCommitOp.name
    )
  }

  def getSinglePartitionInfoForVersion(table_id: String, range_value: String, version: Int): Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val info = dbManager.getSinglePartitionInfo(table_id, range_value, version)
    partitionVersionBuffer += PartitionInfo(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshotList.asScala.map(str => UUID.fromString(str)).toArray,
      expression = info.getExpression,
      commit_op = info.getCommitOp.name
    )
    partitionVersionBuffer.toArray

  }

  def getOnePartitionVersions(table_id: String, range_value: String): Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val res_itr = dbManager.getOnePartitionVersions(table_id, range_value).iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfo(
        table_id = res.getTableId,
        range_value = res.getPartitionDesc,
        version = res.getVersion,
        expression = res.getExpression,
        commit_op = res.getCommitOp.name
      )
    }
    partitionVersionBuffer.toArray

  }

  def getLastedTimestamp(table_id: String, range_value: String): Long = {
    dbManager.getLastedTimestamp(table_id, range_value)
  }

  def getLastedVersionUptoTime(table_id: String, range_value: String, utcMills: Long): Int = {
    dbManager.getLastedVersionUptoTime(table_id, range_value, utcMills)
  }

  /*
  if range_value is "", clean up all patitions;
  if not "" , just one partition
  */
  def cleanMetaUptoTime(table_id: String, range_value: String, utcMills: Long): List[String] = {
    dbManager.getDeleteFilePath(table_id, range_value, utcMills).asScala.toList
  }

  def getPartitionId(table_id: String, range_value: String): (Boolean, String) = {
    (false, "")
  }

  def getAllPartitionInfo(table_id: String): Array[PartitionInfo] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
    val res_itr = dbManager.getAllPartitionInfo(table_id).iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfo(
        table_id = res.getTableId,
        range_value = res.getPartitionDesc,
        version = res.getVersion,
        read_files = res.getSnapshotList.asScala.map(str => UUID.fromString(str)).toArray,
        expression = res.getExpression,
        commit_op = res.getCommitOp.name
      )
    }
    partitionVersionBuffer.toArray
  }

  def rollbackPartitionInfoByVersion(table_id: String, range_value: String, toVersion: Int): Unit = {
    dbManager.rollbackPartitionByVersion(table_id, range_value, toVersion);
  }

  def updateTableSchema(table_name: String,
                        table_id: String,
                        table_schema: String,
                        config: Map[String, String],
                        new_read_version: Int): Unit = {
    dbManager.updateTableSchema(table_id, table_schema)
  }

  //  def deleteTableInfo(table_name: String, table_id: String): Unit = {
  //    deleteTableInfo(table_name, table_id, LakeSoulCatalog.showCurrentNamespace().mkString("."))
  //  }

  def deleteTableInfo(table_name: String, table_id: String, table_namespace: String): Unit = {
    dbManager.deleteTableInfo(table_name, table_id, table_namespace)
  }

  def deletePartitionInfoByTableId(table_id: String): Unit = {
    dbManager.logicDeletePartitionInfoByTableId(table_id)
  }

  def deletePartitionInfoByRangeId(table_id: String, range_value: String, range_id: String): Unit = {
    dbManager.logicDeletePartitionInfoByRangeId(table_id, range_value)
  }

  def dropNamespaceByNamespace(namespace: String): Unit = {
    dbManager.deleteNamespace(namespace)
  }

  def dropPartitionInfoByTableId(table_id: String): Unit = {
    dbManager.deletePartitionInfoByTableId(table_id)
  }

  def dropPartitionInfoByRangeId(table_id: String, range_value: String): Unit = {
    dbManager.deletePartitionInfoByTableAndPartition(table_id, range_value)
  }

  //  def deleteShortTableName(short_table_name: String, table_name: String): Unit = {
  //    deleteShortTableName(short_table_name, table_name, LakeSoulCatalog.showCurrentNamespace().mkString("."))
  //  }

  def deleteShortTableName(short_table_name: String, table_name: String, table_namespace: String): Unit = {
    dbManager.deleteShortTableName(short_table_name, table_name, table_namespace)
  }


  def updateTableShortName(table_name: String,
                           table_id: String,
                           short_table_name: String,
                           table_namespace: String): Unit = {
    dbManager.updateTableShortName(table_name, table_id, short_table_name, table_namespace)
  }

  def cleanMeta(): Unit = {
    dbManager.cleanMeta()
  }

}
