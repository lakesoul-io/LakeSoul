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


  override def addBatch(batchId: Long, data: DataFrame): Unit =
    snapshotManagement.withNewTransaction(tc => {

      val queryId = sqlContext.sparkContext.getLocalProperty(StreamExecution.QUERY_ID_KEY)
      assert(queryId != null)

      if (SchemaUtils.typeExistsRecursively(data.schema)(_.isInstanceOf[NullType])) {
        throw LakeSoulErrors.streamWriteNullTypeException
      }

      val tableInfo = tc.tableInfo
//      if (StreamingRecord.getBatchId(tableInfo.table_id, queryId) >= batchId) {
//        logInfo(s"== Skipping already complete batch $batchId, in query $queryId")
//        return
//      }

      // Streaming sinks can't blindly overwrite schema.
      updateMetadata(
        tc,
        data,
        configuration = Map.empty,
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

  override def toString: String = s"LakeSoulSink[${snapshotManagement.table_path}]"
}
