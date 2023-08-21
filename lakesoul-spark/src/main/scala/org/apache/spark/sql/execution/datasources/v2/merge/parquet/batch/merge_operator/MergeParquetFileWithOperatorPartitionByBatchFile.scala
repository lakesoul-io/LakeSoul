// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch


class MergeParquetFileWithOperatorPartitionByBatchFile[T](filesInfo: Seq[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]],
                                                          mergeOperatorInfo: Map[String, MergeOperator[Any]],
                                                          defaultMergeOp: MergeOperator[Any])
  extends PartitionReader[InternalRow] with Logging {

  val filesItr: Iterator[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]] = filesInfo.iterator
  var mergeLogic: MergeMultiFileWithOperator = _

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
          mergeLogic = new MergeMultiFileWithOperator(nextFiles, mergeOperatorInfo, defaultMergeOp)
        }
      } else {
        return false
      }
    }

    if (mergeLogic.isHeapEmpty) {
      if (filesItr.hasNext) {
        //close current file readers
        mergeLogic.closeReadFileReader()

        mergeLogic = new MergeMultiFileWithOperator(filesItr.next(), mergeOperatorInfo, defaultMergeOp)
      } else {
        return false
      }
    }

    mergeLogic.merge()
    true
  }

  /**
    * @return InternalRow
    */
  override def get(): InternalRow = {

    if (mergeLogic.isTemporaryRow()) {
      mergeLogic.setTemporaryRowFalse()
      val temporaryRow = mergeLogic.getTemporaryRow()
      val arrayRow = new GenericInternalRow(temporaryRow.clone())
      arrayRow
    } else {
      mergeLogic.getRowByProxyMergeBatch()
    }
  }

  override def close(): Unit = {
    if (filesInfo.nonEmpty) {
      filesInfo.foreach(f => f.foreach(_._2.close()))
    }
  }


}
