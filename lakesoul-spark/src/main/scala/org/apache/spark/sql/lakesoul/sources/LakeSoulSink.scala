// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.sources

import com.dmetasoul.lakesoul.meta.StreamingRecord
import org.apache.hadoop.fs.Path
import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.sql.execution.streaming.{Sink, StreamExecution}
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.{ImplicitMetadataOperation, SchemaUtils}
import org.apache.spark.sql.lakesoul.{SnapshotManagement, LakeSoulOptions}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.{DataFrame, SQLContext}

class LakeSoulSink(sqlContext: SQLContext,
                   path: Path,
                   outputMode: OutputMode,
                   options: LakeSoulOptions)
  extends Sink with ImplicitMetadataOperation {

  private val snapshotManagement = SnapshotManagement(path)

  override protected val canOverwriteSchema: Boolean =
    outputMode == OutputMode.Complete() && options.canOverwriteSchema

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  override val rangePartitions: String = options.rangePartitions
  override val hashPartitions: String = options.hashPartitions
  override val hashBucketNum: Int = options.hashBucketNum
  override val shortTableName: Option[String] = options.shortTableName


  def writeBatch(batchId: Long, data: DataFrame):Unit =    snapshotManagement.withNewTransaction(tc => {
    val queryId = sqlContext.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
    assert(queryId != null)

    if (SchemaUtils.typeExistsRecursively(data.schema)(_.isInstanceOf[NullType])) {
      throw LakeSoulErrors.streamWriteNullTypeException
    }

    val tableInfo = tc.tableInfo
    if (StreamingRecord.getBatchId(tableInfo.table_id, queryId) >= batchId) {
      logInfo(s"== Skipping already complete batch $batchId, in query $queryId")
      return
    }

    // Streaming sinks can't blindly overwrite schema.
    updateMetadata(
      tc,
      data,
      configuration = options.options,
      outputMode == OutputMode.Complete())

    val deletedFiles = outputMode match {
      case o if o == OutputMode.Complete() =>
        snapshotManagement.assertRemovable()
        val operationTimestamp = System.currentTimeMillis()
        tc.filterFiles().map(_.expire(operationTimestamp))
      case _ => Nil
    }

    if (tc.tableInfo.hash_partition_columns.nonEmpty) {
      tc.setCommitType("merge")
    }
    val newFiles = tc.writeFiles(data, Some(options))

    tc.commit(newFiles, deletedFiles, queryId, batchId)

    //clean shuffle data
    val map = sqlContext.sparkContext.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster].shuffleStatuses
    map.keys.foreach(shuffleId => {
      sqlContext.sparkContext.cleaner.get.doCleanupShuffle(shuffleId, blocking = false)
    })

  })

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val rdd = data.queryExecution.toRdd
    implicit val enc = data.exprEnc
    val ds = data.sparkSession.internalCreateDataFrame(rdd, data.schema)
    writeBatch(batchId,ds)
  }

  override def toString: String = s"LakeSoulSink[${snapshotManagement.table_path}]"
}
