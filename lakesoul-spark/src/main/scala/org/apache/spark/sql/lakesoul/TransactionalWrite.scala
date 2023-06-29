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

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.CommitType
import com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.{InvariantCheckerExec, Invariants, SchemaUtils}
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, SparkUtil}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait TransactionalWrite {
  self: Transaction =>
  protected def snapshot: Snapshot

  protected var commitType: Option[CommitType]

  protected var shortTableName: Option[String]

  protected var hasWritten = false

  protected def getCommitter(outputPath: Path): DelayedCommitProtocol =
    new DelayedCommitProtocol("lakesoul", outputPath.toString, None)

  /**
    * Normalize the schema of the query, and return the QueryExecution to execute. The output
    * attributes of the QueryExecution may not match the attributes we return as the output schema.
    * This is because streaming queries create `IncrementalExecution`, which cannot be further
    * modified. We can however have the Parquet writer use the physical plan from
    * `IncrementalExecution` and the output schema provided through the attributes.
    */
  protected def normalizeData(data: Dataset[_]): (QueryExecution, Seq[Attribute]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(tableInfo.schema, data)
    val cleanedData = SchemaUtils.dropNullTypeColumns(normalizedData)
    val queryExecution = if (cleanedData.schema != normalizedData.schema) {
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else {
      // For streaming workloads, we need to use the QueryExecution created from StreamExecution
      data.queryExecution
    }
    queryExecution -> cleanedData.queryExecution.analyzed.output
  }


  protected def getPartitioningColumns(rangePartitionSchema: StructType,
                                       hashPartitionSchema: StructType,
                                       output: Seq[Attribute],
                                       colsDropped: Boolean): Seq[Attribute] = {
    val rangePartitionColumns: Seq[Attribute] = rangePartitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      output.find(f => f.name == col.name)
        .getOrElse {
          throw LakeSoulErrors.partitionColumnNotFoundException(col.name, output)
        }
    }
    hashPartitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      output.find(f => f.name == col.name)
        .getOrElse {
          throw LakeSoulErrors.partitionColumnNotFoundException(col.name, output)
        }
    }

    if (rangePartitionColumns.nonEmpty && rangePartitionColumns.length == output.length) {
      throw LakeSoulErrors.nonPartitionColumnAbsentException(colsDropped)
    }
    rangePartitionColumns
  }

  def writeFiles(data: Dataset[_]): Seq[DataFileInfo] = writeFiles(data, None, isCompaction = false)._1

  def writeFiles(data: Dataset[_], writeOptions: Option[LakeSoulOptions]): Seq[DataFileInfo] =
    writeFiles(data, writeOptions, isCompaction = false)._1

  def writeFiles(data: Dataset[_], isCompaction: Boolean): (Seq[DataFileInfo], Path) =
    writeFiles(data, None, isCompaction = isCompaction)

  /**
    * Writes out the dataframe after performing schema validation. Returns a list of
    * actions to append these files to the reservoir.
    */
  def writeFiles(oriData: Dataset[_],
                 writeOptions: Option[LakeSoulOptions],
                 isCompaction: Boolean): (Seq[DataFileInfo], Path) = {
    val data = if (!isCompaction && tableInfo.hash_partition_columns.nonEmpty) {
      oriData.repartition(tableInfo.bucket_num, tableInfo.hash_partition_columns.map(col): _*)
    } else {
      oriData
    }

    hasWritten = true
    val spark = data.sparkSession
    spark.sessionState.conf.setConfString(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key, "false")

    //If this is the first time to commit, you need to check if there is data in the path where the table is located.
    //If there has data, you cannot create a new table
    if (isFirstCommit) {
      val path = new Path(table_path)
      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
        throw LakeSoulErrors.failedCreateTableException(table_path)
      }
    }

    val rangePartitionSchema = tableInfo.range_partition_schema
    val hashPartitionSchema = tableInfo.hash_partition_schema
    var outputPath = SparkUtil.makeQualifiedTablePath(tableInfo.table_path)
    if (isCompaction) {
      outputPath = SparkUtil.makeQualifiedTablePath(new Path(tableInfo.table_path.toString + "/compact_" + System.currentTimeMillis()))
    }

    val (queryExecution, output) = normalizeData(data)
    val partitioningColumns =
      getPartitioningColumns(
        rangePartitionSchema,
        hashPartitionSchema,
        output,
        output.length < data.schema.size)

    val committer = getCommitter(outputPath)

    //add not null check to primary key
    val invariants = Invariants.getFromSchema(tableInfo.schema, spark)

    SQLExecution.withNewExecutionId(queryExecution) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      val physicalPlan = if (isCompaction) {
        queryExecution.executedPlan
      } else {
        InvariantCheckerExec(queryExecution.executedPlan, invariants)
      }

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
        new SerializableConfiguration(spark.sessionState.newHadoopConf()),
        BasicWriteJobStatsTracker.metrics)
      statsTrackers.append(basicWriteJobStatsTracker)


      val hashBucketSpec = tableInfo.hash_column match {
        case "" => None
        case _ => Option(BucketSpec(tableInfo.bucket_num,
          tableInfo.hash_partition_columns,
          tableInfo.hash_partition_columns))
      }

      val sqlConf = spark.sessionState.conf
      val writeOptions = new mutable.HashMap[String, String]()
      if (sqlConf.getConf(LakeSoulSQLConf.PARQUET_COMPRESSION_ENABLE)) {
        writeOptions.put("compression", sqlConf.getConf(LakeSoulSQLConf.PARQUET_COMPRESSION))
      } else {
        writeOptions.put("compression", "uncompressed")
      }

      //      Map("parquet.block.size" -> spark.sessionState.conf.getConf(LakeSoulSQLConf.PARQUET_BLOCK_SIZE).toString)

      FileFormatWriter.write(
        sparkSession = spark,
        plan = physicalPlan,
        fileFormat = snapshot.fileFormat, // TODO doesn't support changing formats.
        committer = committer,
        outputSpec = outputSpec,
        hadoopConf = spark.sessionState.newHadoopConfWithOptions(snapshot.getConfiguration),
        partitionColumns = partitioningColumns,
        bucketSpec = hashBucketSpec,
        statsTrackers = statsTrackers,
        options = writeOptions.toMap)
    }
    val partitionCols = tableInfo.range_partition_columns
    //Returns the absolute path to the file
    val real_write_cols = data.schema.fieldNames.filter(!partitionCols.contains(_)).mkString(LAKESOUL_RANGE_PARTITION_SPLITTER)
    (committer.addedStatuses.map(file => file.copy(
      file_exist_cols = real_write_cols
    )), outputPath)
  }


}
