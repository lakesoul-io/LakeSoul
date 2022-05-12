/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul.utils

import java.util.UUID

import com.dmetasoul.lakesoul.meta.{CommitState, CommitType, MetaUtils}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.lakesoul.LakeSoulTableProperties
import org.apache.spark.sql.lakesoul.material_view.QueryInfo
import org.apache.spark.sql.types.{DataType, StructType}

case class MetaInfo(table_info: TableInfo,
                    partitionInfoArray: Array[PartitionInfo],
                    commit_type: CommitType,
                    commit_id: String = "",
                    query_id: String = "",
                    batch_id: Long = -1L)

case class PartitionInfo(table_id: String,
                         range_value: String,
                         version: Int,
                         read_files: Array[UUID] = Array.empty[UUID],
                         expression:String=""
                      ) {
  override def toString: String = {
    s"partition info: {\ntable_name: $table_id,\nrange_value: $range_value}"
  }
}

// table_schema is json format data
// range_column and hash_column are string， not json format
//hash_partition_column contains multi keys，concat with `,`
//table name -> tablepath
//shorttablename -> tablename
case class TableInfo(table_name: String,
                     table_id: String,
                     table_schema: String = null,
                     range_column: String = "",
                     hash_column: String = "",
                     bucket_num: Int = -1,
                     configuration: Map[String, String] = Map.empty,
                     short_table_name: Option[String] = None
                     ) {

  lazy val table_path: Path = new Path(table_name)
  lazy val range_partition_columns: Seq[String] = range_partition_schema.fieldNames
  lazy val hash_partition_columns: Seq[String] = hash_partition_schema.fieldNames

  /** Returns the schema as a [[StructType]] */
  //full table schema which contains partition columns
  @JsonIgnore
  lazy val schema: StructType =
  Option(table_schema).map { s =>
    DataType.fromJson(s).asInstanceOf[StructType]
  }.getOrElse(StructType.apply(Nil))

  //range partition columns
  @JsonIgnore
  lazy val range_partition_schema: StructType =
  if (range_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(range_column.split(",").map(c => schema(c)))
  }

  //hash partition columns
  @JsonIgnore
  lazy val hash_partition_schema: StructType =
  if (hash_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(hash_column.split(",").map(c => schema(c)))
  }

  //all partition columns
  lazy val partition_schema: StructType = range_partition_schema.merge(hash_partition_schema)

  //hash is belong to data_schema !!!
  private lazy val range_partition_set: Set[String] = range_column.split(",").toSet
  //all data schema except range partition columns
  @JsonIgnore
  lazy val data_schema: StructType = StructType(schema.filterNot(f => range_partition_set.contains(f.name)))

  lazy val partition_cols: Seq[String] = {
    var seq = Seq.empty[String]
    if (range_column.nonEmpty) {
      seq = seq ++ range_column.split(",")
    }
    if (hash_column.nonEmpty) {
      seq = seq ++ hash_column.split(",")
    }
    seq
  }

  lazy val format: Format = Format()
}

case class DataFileOp(
                     path:String,
                     file_op:String
                     )
//single file info
case class DataFileInfo(table_id: String,
                        range_value: String,
                        commit_id: String,
                        size: Long,
                        modification_time: Long,
                        commit_type: String,
                        fileops:Array[DataFileOp]=Array.empty[DataFileOp]
                      ) {
  lazy val range_key: String = range_value

  //identify for merge read
  lazy val range_version: String = range_key

  lazy val file_bucket_id: Int = BucketingUtils
    .getBucketId(new Path(file_path).getName)
    .getOrElse(sys.error(s"Invalid bucket file $file_path"))

  //trans to files which need to delete
  def expire(deleteTime: Long): DataFileInfo = this.copy(modification_time = deleteTime)
}


case class PartitionFilterInfo(range_id: String,
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