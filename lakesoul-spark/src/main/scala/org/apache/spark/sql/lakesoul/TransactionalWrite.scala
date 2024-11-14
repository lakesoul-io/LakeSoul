// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_EMPTY_STRING, LAKESOUL_NULL_STRING, LAKESOUL_RANGE_PARTITION_SPLITTER}
import com.dmetasoul.lakesoul.meta.{CommitType, DataFileInfo}
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.LakeSoulFileWriter.{COPY_FILE_WRITER_KEY, COPY_FILE_WRITER_SPLITTER}
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, LakeSoulFileWriter, WriteJobStatsTracker}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.{InvariantCheckerExec, Invariants, SchemaUtils}
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.types.{StringType, StructType}
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
    new DelayedCommitProtocol("lakesoul", outputPath.toUri.toString, None)

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

  def addRenameFiles(files: Seq[(Path, Path)]): Unit = {
    files.foreach(srcAndDst => addRenameFile(srcAndDst._1, srcAndDst._2))
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
                 isCompaction: Boolean,
                 copyCompactedFile: Seq[DataFileInfo] = Seq.empty): (Seq[DataFileInfo], Path) = {
    val spark = oriData.sparkSession
    // LakeSoul always writes timestamp data with timezone=UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sharedState.cacheManager.uncacheQuery(oriData, true)

    val newBucketNum = if (writeOptions.isDefined) {
      writeOptions.get.options.get("newBucketNum").map(_.toInt)
    } else {
      None
    }
    val bucketNumChanged = newBucketNum.exists(tableInfo.bucket_num != _)

    val data = Dataset.ofRows(spark, (if (!isCompaction && tableInfo.hash_partition_columns.nonEmpty) {
      oriData.repartition(tableInfo.bucket_num, tableInfo.hash_partition_columns.map(col): _*)
    } else {
      if (isCompaction && bucketNumChanged && tableInfo.hash_partition_columns.nonEmpty) {
        oriData.repartition(newBucketNum.get, tableInfo.hash_partition_columns.map(col): _*)
      } else {
        oriData
      }
    }).logicalPlan)

    hasWritten = true
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

    val options = new mutable.HashMap[String, String]()
    val rangePartitionSchema = tableInfo.range_partition_schema
    val rangePartitionCols = rangePartitionSchema.map(f => (f.name, f.dataType))
    val hashPartitionSchema = tableInfo.hash_partition_schema
    var outputPath = SparkUtil.makeQualifiedTablePath(tableInfo.table_path)
    if (isCompaction) {

      val compactionPath = if (writeOptions.isDefined) {
        writeOptions.get.options.getOrElse("compactionPath", tableInfo.table_path.toString + "/compact_" + System.currentTimeMillis())
      } else {
        tableInfo.table_path.toString + "/compact_" + System.currentTimeMillis()
      }
      outputPath = SparkUtil.makeQualifiedTablePath(new Path(compactionPath))
    }
    val dc = if (isCompaction) {
      if (bucketNumChanged) {
        options.put("isBucketNumChanged", "true")
      } else {
        options.put("isBucketNumChanged", "false")
      }
      val cdcCol = snapshot.getTableInfo.configuration.get(LakeSoulTableProperties.lakeSoulCDCChangePropKey)
      if (cdcCol.nonEmpty && copyCompactedFile.isEmpty) {
        options.put("isCDC", "true")
        val cdcColName = cdcCol.get
        if (writeOptions.forall(_.options.getOrElse("fullCompaction", "true").equals("true"))) {
          data.withColumn(cdcColName,
            when(col(cdcColName) === "update", "insert")
              .otherwise(col(cdcColName))
          ).where(s"$cdcColName != 'delete'")
        } else {
          data.withColumn(cdcColName,
            when(col(cdcColName) === "update", "insert")
              .otherwise(col(cdcColName))
          )
        }
      } else {
        data
      }
    } else {
      data
    }
    spark.sharedState.cacheManager.uncacheQuery(dc, true)
    // for compaction we don't need to change partition values
    val dp = if (rangePartitionCols.nonEmpty && !isCompaction) {
      var dataset = dc
      rangePartitionCols.foreach(p => {
        if (p._2 == StringType) {
          val name = p._1
          dataset = dataset.withColumn(name,
            when(col(name) === "", LAKESOUL_EMPTY_STRING)
              .when(col(name).isNull, LAKESOUL_NULL_STRING)
              .otherwise(col(name))
          )
        }
      })
      dataset
    } else {
      dc
    }
    val dataset = Dataset.ofRows(spark, dp.logicalPlan)

    val (queryExecution, output) = if (isCompaction) {
      dataset.queryExecution -> dataset.queryExecution.analyzed.output
    } else {
      normalizeData(dataset)
    }
    val partitioningColumns = {
      if (isCompaction) Seq.empty else
        getPartitioningColumns(
          rangePartitionSchema,
          hashPartitionSchema,
          output,
          output.length < data.schema.size)
    }

    val committer = if (copyCompactedFile.nonEmpty) {
      options.put(COPY_FILE_WRITER_KEY, copyCompactedFile.map(_.path).mkString(COPY_FILE_WRITER_SPLITTER))
      new DelayedCopyCommitProtocol(copyCompactedFile, "lakesoul", outputPath.toString, None)
    } else {
      getCommitter(outputPath)
    }

    SQLExecution.withNewExecutionId(queryExecution) {
      val outputSpec = LakeSoulFileWriter.OutputSpec(
        outputPath.toUri.toString,
        Map.empty,
        output)

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
        new SerializableConfiguration(spark.sessionState.newHadoopConf()),
        BasicWriteJobStatsTracker.metrics)
      statsTrackers.append(basicWriteJobStatsTracker)


      val hashBucketSpec = tableInfo.hash_column match {
        case "" => None
        case _ if bucketNumChanged => Option(BucketSpec(newBucketNum.get,
          tableInfo.hash_partition_columns,
          tableInfo.hash_partition_columns))
        case _ => Option(BucketSpec(tableInfo.bucket_num,
          tableInfo.hash_partition_columns,
          tableInfo.hash_partition_columns))
      }

      val sqlConf = spark.sessionState.conf
      writeOptions.map(options ++= _.options)

      if (sqlConf.getConf(LakeSoulSQLConf.PARQUET_COMPRESSION_ENABLE)) {
        options.put("compression", sqlConf.getConf(LakeSoulSQLConf.PARQUET_COMPRESSION))
      } else {
        options.put("compression", "uncompressed")
      }

      val physicalPlan = if (isCompaction) {
        queryExecution.executedPlan
      } else {
        val invariants = Invariants.getFromSchema(tableInfo.schema, spark)
        InvariantCheckerExec(queryExecution.executedPlan, invariants)
      }

      LakeSoulFileWriter.write(
        sparkSession = spark,
        plan = physicalPlan,
        fileFormat = snapshot.fileFormat,
        committer = committer,
        outputSpec = outputSpec,
        hadoopConf = spark.sessionState.newHadoopConfWithOptions(snapshot.getConfiguration),
        partitionColumns = partitioningColumns,
        bucketSpec = hashBucketSpec,
        statsTrackers = statsTrackers,
        options = options.toMap)
    }
    val partitionCols = tableInfo.range_partition_columns
    //Returns the absolute path to the file
    val real_write_cols = data.schema.fieldNames.filter(!partitionCols.contains(_)).mkString(LAKESOUL_RANGE_PARTITION_SPLITTER)
    (committer.addedStatuses.map(file => file.copy(
      file_exist_cols = real_write_cols
    )), outputPath)
  }
}
