// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeParquetSingletonFilePartitionByBatchFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{MergeOperator, MergeParquetFileWithOperatorPartitionByBatchFile}
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeFilePartition, MergeFilePartitionReader, MergePartitionedFile, MergePartitionedFileReader}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class MergeFilePartitionReaderFactory(mergeOperatorInfo: Map[String, MergeOperator[Any]], defaultMergeOp: MergeOperator[Any])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]

    val iter = filePartition.files.toIterator.map { files =>
      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      files.map(f => f -> buildColumnarReader(f)).toSeq
    }.toSeq

    val mergeReader =
      if (filePartition.isSingleFile) {
        new MergeParquetSingletonFilePartitionByBatchFile[InternalRow](iter)
      } else {
        new MergeParquetFileWithOperatorPartitionByBatchFile[InternalRow](iter, mergeOperatorInfo, defaultMergeOp)
      }

    new MergeFilePartitionReader[InternalRow](
      Iterator(MergePartitionedFileReader( //filePartition.files.head,
        mergeReader))
    )
  }

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {

    throw new Exception("this function is not supported")
  }

  def buildReader(partitionedFile: MergePartitionedFile): PartitionReader[InternalRow]

  def buildColumnarReader(partitionedFile: MergePartitionedFile): PartitionReader[ColumnarBatch] = {
    throw new UnsupportedOperationException("Cannot create columnar reader.")
  }
}
