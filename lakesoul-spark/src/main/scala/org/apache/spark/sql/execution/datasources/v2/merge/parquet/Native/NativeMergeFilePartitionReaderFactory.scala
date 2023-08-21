// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReader, PartitionedFileReader}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeParquetSingletonFilePartitionByBatchFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{MergeOperator, MergeParquetFileWithOperatorPartitionByBatchFile}
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeFilePartition, MergeFilePartitionReader, MergePartitionedFile, MergePartitionedFileReader}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class NativeMergeFilePartitionReaderFactory(mergeOperatorInfo: Map[String, MergeOperator[Any]], defaultMergeOp: MergeOperator[Any])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]

    val iter = filePartition.files.toIterator.map { files =>
      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      files.map(f => f -> buildColumnarReader(Seq(f))).toSeq
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
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]
    val iter = filePartition.files.toIterator.map { files =>
      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      MergePartitionedFileReader(buildColumnarReader(files))
    }
    new MergeFilePartitionReader[ColumnarBatch](iter)
  }

  def buildReader(partitionedFile: MergePartitionedFile): PartitionReader[InternalRow]

  def buildColumnarReader(partitionedFile: Seq[MergePartitionedFile]): PartitionReader[ColumnarBatch]

  override def supportColumnarReads(partition: InputPartition): Boolean = true

}
