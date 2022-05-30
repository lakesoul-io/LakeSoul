/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.spark.sql.lakesoul.utils

import com.dmetasoul.lakesoul.meta.{DataOperation, MetaUtils}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.lakesoul.{BatchDataSoulFileIndexV2, PartitionFilter, SnapshotManagement}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.sources.LakeSoulBaseRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.lakesoul.Snapshot
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import scala.collection.JavaConverters._


object SparkUtil {
  lazy val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  def allPartitionFilterInfoDF(snapshot : Snapshot):  DataFrame = {
   val allPatition= snapshot.getPartitionInfoArray.map(part =>
        PartitionFilterInfo(
          part.range_value,
          MetaUtils.getPartitionMapFromKey(part.range_value),
          part.version))

    spark.sparkContext.parallelize(allPatition).toDF().persist()
  }

  def allDataInfo(snapshot: Snapshot): Array[DataFileInfo] = {
    import spark.implicits._
    spark.sparkContext.parallelize(DataOperation.getTableDataInfo(snapshot.getPartitionInfoArray)).toDS().persist().as[DataFileInfo].collect()
  }

/*  def modifyTableString(tablePath: String): String = {
    makeQualifiedTablePath(tablePath).toString
  }

  def modifyTablePath(tablePath: String): Path = {
    makeQualifiedTablePath(tablePath)
  }

  private def makeQualifiedTablePath(tablePath: String): Path = {
    val normalPath = tablePath.replace("s3://", "s3a://")
    val path = new Path(normalPath)
    path.getFileSystem(spark.sessionState.newHadoopConf()).makeQualified(path)
  }*/

  def makeQualifiedTablePath(tablePath: Path): Path = {
    val normalPath = tablePath.toString.replace("s3://", "s3a://")
    val path = new Path(normalPath)
    path.getFileSystem(spark.sessionState.newHadoopConf()).makeQualified(path)
  }

  def TablePathExisted(fs:FileSystem ,tableAbsolutePath:Path):Boolean = {
    if (fs.exists(tableAbsolutePath) && fs.listStatus(tableAbsolutePath).nonEmpty) {
        true
    }else{
      false
    }
  }

  // ------------------snapshotmanagement--------------
  def createRelation(partitionFilters: Seq[Expression] = Nil,snapmnt:SnapshotManagement,sparksess:SparkSession): BaseRelation = {
    val files: Array[DataFileInfo] = PartitionFilter.filesForScan(snapmnt.snapshot, partitionFilters)
    LakeSoulBaseRelation(files, snapmnt)(sparksess)
  }


  def createDataFrame(files: Seq[DataFileInfo],
                      requiredColumns: Seq[String],
                      snapmnt:SnapshotManagement,
                      predicts: Option[Expression] = None): DataFrame = {
    val skipFiles = if (predicts.isDefined) {
      val predictFiles = PartitionFilter.filesForScan(snapmnt.snapshot, Seq(predicts.get))
      files.intersect(predictFiles)
    } else {
      files
    }
  val table_name=snapmnt.table_name
    val fileIndex = BatchDataSoulFileIndexV2(spark, snapmnt, skipFiles)
    val table = LakeSoulTableV2(
      spark,
      new Path(table_name),
      None,
      None,
      Option(fileIndex)
    )
    val option = new CaseInsensitiveStringMap(Map("basePath" -> table_name).asJava)
    Dataset.ofRows(
      spark,
      DataSourceV2Relation(
        table,
        table.schema().toAttributes,
        None,
        None,
        option
      )
    ).select(requiredColumns.map(col): _*)
  }
}
