// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.{FilePartitionReader, PartitionedFileReader}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class NativeFilePartitionReaderFactory extends PartitionReaderFactory with Logging{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Cannot create row-based reader.")
  }

  def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow]

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = {
    assert(partition.isInstanceOf[FilePartition])
    val filePartition = partition.asInstanceOf[FilePartition]
    val iter = filePartition.files.toIterator.map { file =>
      PartitionedFileReader(file, buildColumnarReader(file))
    }
    new FilePartitionReader[ColumnarBatch](iter)
  }

  def buildColumnarReader(partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch]
}
