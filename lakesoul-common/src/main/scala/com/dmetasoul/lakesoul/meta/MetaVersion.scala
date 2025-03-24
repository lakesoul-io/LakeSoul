// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.alibaba.fastjson.JSONObject
import com.dmetasoul.lakesoul.meta.entity.PartitionInfo

import java.util
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

  def listTables(namespace: Array[String]): util.List[String] = {
    dbManager.listTablePathsByNamespace(namespace.mkString("."))
  }

  def getSinglePartitionInfo(table_id: String, range_value: String, range_id: String): PartitionInfoScala = {
    val info = dbManager.getSinglePartitionInfo(table_id, range_value)
    PartitionInfoScala(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshotList.asScala.map(DBUtil.toJavaUUID).toArray,
      expression = info.getExpression,
      commit_op = info.getCommitOp.name
    )
  }

  def getSinglePartitionInfoForVersion(table_id: String, range_value: String, version: Int): Array[PartitionInfoScala] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfoScala]()
    val info = dbManager.getSinglePartitionInfo(table_id, range_value, version)
    partitionVersionBuffer += PartitionInfoScala(
      table_id = info.getTableId,
      range_value = range_value,
      version = info.getVersion,
      read_files = info.getSnapshotList.asScala.map(DBUtil.toJavaUUID).toArray,
      expression = info.getExpression,
      commit_op = info.getCommitOp.name
    )
    partitionVersionBuffer.toArray

  }

  def getOnePartitionVersions(table_id: String, range_value: String): Array[PartitionInfoScala] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfoScala]()
    val res_itr = dbManager.getOnePartitionVersions(table_id, range_value).iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfoScala(
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

  def getAllPartitionInfo(table_id: String): util.List[PartitionInfo] = {
    dbManager.getAllPartitionInfo(table_id)
  }

  def getAllPartitionDesc(table_id: String, table_partition_cols: Seq[String] = Seq.empty,
                          equalityFilter: Seq[(String, String)] = Seq.empty): util.List[String] = {
    if (equalityFilter.isEmpty || table_partition_cols.isEmpty) {
      dbManager.getTableAllPartitionDesc(table_id)
    } else if (table_partition_cols.forall(col => equalityFilter.indexWhere(_._1.equals(col)) != -1)) {
      // all equality filter, match exact one partition desc
      val partitionInfo = dbManager.getOnePartition(table_id,
        equalityFilter.map(f => f._1 + "=" + f._2).mkString(","))
      if (partitionInfo == null || partitionInfo.isEmpty) {
        util.Collections.emptyList()
      } else {
        util.Collections.singletonList(partitionInfo.get(0).getPartitionDesc)
      }
    } else {
      // partial equality filter, use gin tsvector
      dbManager.getPartitionDescByPartialFilter(table_id,
        equalityFilter.map(f => f._1 + "=" + f._2).mkString(" & "))
    }
  }

  def convertPartitionInfoScala(partitionList: util.List[PartitionInfo]): Array[PartitionInfoScala] = {
    val partitionVersionBuffer = new ArrayBuffer[PartitionInfoScala]()
    val res_itr = partitionList.iterator()
    while (res_itr.hasNext) {
      val res = res_itr.next()
      partitionVersionBuffer += PartitionInfoScala(
        table_id = res.getTableId,
        range_value = res.getPartitionDesc,
        version = res.getVersion,
        read_files = res.getSnapshotList.asScala.map(DBUtil.toJavaUUID).toArray,
        expression = res.getExpression,
        commit_op = res.getCommitOp.name
      )
    }
    partitionVersionBuffer.toArray
  }

  def getAllPartitionInfoScala(table_id: String): Array[PartitionInfoScala] = {
    convertPartitionInfoScala(getAllPartitionInfo(table_id))
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
