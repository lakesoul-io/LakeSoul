// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.utils

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_HASH_PARTITION_SPLITTER, LAKESOUL_RANGE_PARTITION_SPLITTER}
import com.dmetasoul.lakesoul.meta.dao.TableInfoDao
import com.dmetasoul.lakesoul.meta.{CommitState, CommitType, DataFileInfo, PartitionInfoScala}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.arrow.ArrowUtils
import org.apache.spark.sql.types.{DataType, StructType}

import java.util.UUID

case class MetaInfo(table_info: TableInfo,
                    partitionInfoArray: Array[PartitionInfoScala],
                    dataCommitInfo: Array[DataCommitInfo],
                    commit_type: CommitType,
                    commit_id: String = "",
                    query_id: String = "",
                    batch_id: Long = -1L,
                    readPartitionInfo: Array[PartitionInfoScala] = null)


case class Format(provider: String = "parquet",
                  options: Map[String, String] = Map.empty)

// table_schema is json format data
// range_column and hash_column are string， not json format ; hash_partition_column contains multi keys，concat with `,`
case class TableInfo(namespace: String,
                     table_path_s: Option[String] = None,
                     table_id: String,
                     table_schema: String = null,
                     range_column: String = "",
                     hash_column: String = "",
                     bucket_num: Int = -1,
                     configuration: Map[String, String] = Map.empty,
                     short_table_name: Option[String] = None
                    ) {

  lazy val table_path: Path = SparkUtil.makeQualifiedTablePath(new Path(table_path_s.get))
  lazy val range_partition_columns: Seq[String] = range_partition_schema.fieldNames
  lazy val hash_partition_columns: Seq[String] = hash_partition_schema.fieldNames

  /** Returns the schema as a [[StructType]] */
  //full table schema which contains partition columns
  @JsonIgnore
  lazy val schema: StructType =
  Option(table_schema).map { s => {
    // latest version: from arrow schema json
    if (TableInfoDao.isArrowKindSchema(s))
      ArrowUtils.fromArrowSchema(Schema.fromJSON(s))
    else
    // old version: from spark struct datatype
      DataType.fromJson(s).asInstanceOf[StructType]
  }
  }.getOrElse(StructType.apply(Nil))

  //range partition columns
  @JsonIgnore
  lazy val range_partition_schema: StructType =
  if (range_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(range_column.split(LAKESOUL_RANGE_PARTITION_SPLITTER).map(c => schema(c)))
  }

  //hash partition columns
  @JsonIgnore
  lazy val hash_partition_schema: StructType =
  if (hash_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(hash_column.split(LAKESOUL_HASH_PARTITION_SPLITTER).map(c => schema(c)))
  }

  //all partition columns
  lazy val partition_schema: StructType = range_partition_schema.merge(hash_partition_schema)

  //hash is belong to data_schema !!!
  private lazy val range_partition_set: Set[String] = range_column.split(LAKESOUL_RANGE_PARTITION_SPLITTER).toSet
  //all data schema except range partition columns
  @JsonIgnore
  lazy val data_schema: StructType = StructType(schema.filterNot(f => range_partition_set.contains(f.name)))

  lazy val partition_cols: Seq[String] = {
    var seq = Seq.empty[String]
    if (range_column.nonEmpty) {
      seq = seq ++ range_column.split(LAKESOUL_RANGE_PARTITION_SPLITTER)
    }
    if (hash_column.nonEmpty) {
      seq = seq ++ hash_column.split(LAKESOUL_HASH_PARTITION_SPLITTER)
    }
    seq
  }

  lazy val format: Format = Format()
}

//single file info
case class DataCommitInfo(table_id: String,
                          range_value: String,
                          commit_id: UUID,
                          commit_type: String,
                          modification_time: Long = -1L,
                          file_ops: Array[DataFileInfo] = Array.empty[DataFileInfo]
                         ) {
  lazy val range_key: String = commit_id.toString
  //identify for merge read
  lazy val range_version: String = range_key
}


case class PartitionFilterInfo(
                                range_value: String,
                                range_partitions: Map[String, String],
                                read_version: Long)


/**
  * commit state info
  *
  * @param state     commit state
  * @param commit_id commit id
  * @param tag       identifier to redo or rollback
  * @param timestamp timestamp of commit
  */
case class commitStateInfo(state: CommitState.Value,
                           table_name: String,
                           table_id: String,
                           commit_id: String,
                           tag: Int,
                           timestamp: Long)


case class RelationTable(tableName: String,
                         tableId: String,
                         partitionInfo: Seq[(String, String)]) {
  override def toString: String = {
    tableName + "\001" + tableId + "\001" + partitionInfo.sortBy(_._1).map(m => m._1 + "->" + m._2).mkString("\002")
  }

}

object RelationTable {
  def build(relationTables: String): RelationTable = {
    val split = relationTables.split("\001")
    val tableName = split(0)
    val tableId = split(1)
    val partitionInfo = split(2).split("\002").map(m => {
      val part = m.split("->")
      (part(0), part(1))
    })
    RelationTable(tableName, tableId, partitionInfo)
  }
}