// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
  * merge on multi partition files
  *
  * @param filesInfo Seq(Seq()) => rangePartitions(filesInOnePartition())
  * @tparam T
  */
class MergeParquetSingletonFilePartitionByBatchFile[T](filesInfo: Seq[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]])
  extends PartitionReader[InternalRow] with Logging {

  val filesItr: Iterator[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]] = filesInfo.iterator
  var mergeLogic: MergeSingletonFile = _

  /**
    * @return Boolean
    */
  override def next(): Boolean = {
    if (mergeLogic == null) {
      if (filesItr.hasNext) {
        val nextFiles = filesItr.next()
        if (nextFiles.isEmpty) {
          return false
        } else {
          mergeLogic = new MergeSingletonFile(nextFiles)
        }
      } else {
        return false
      }
    }

    if (mergeLogic.deDuplication()) {
      true
    } else if (filesItr.hasNext) {
      //close current file readers
      mergeLogic.closeReadFileReader()

      mergeLogic = new MergeSingletonFile(filesItr.next())
      mergeLogic.deDuplication()
    } else {
      false
    }
  }

  /**
    * @return InternalRow
    */
  override def get(): InternalRow = {
    mergeLogic.getRow()
  }

  override def close() = if (filesInfo.nonEmpty) {
    filesInfo.foreach(f => f.foreach(_._2.close()))
  }

}
