package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeParquetSingletonFilePartitionByBatchFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeParquetFileWithOperatorPartitionByBatchFile
import org.apache.spark.sql.execution.datasources.v2.merge.{MergeFilePartition, MergeFilePartitionReader, MergePartitionedFile, MergePartitionedFileReader}
import org.apache.spark.sql.execution.datasources.v2.parquet.Native.NativeFilePartitionReader
import org.apache.spark.sql.vectorized.ColumnarBatch


abstract class NativeMergeFilePartitionReaderFactory extends PartitionReaderFactory with Logging{
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    assert(partition.isInstanceOf[MergeFilePartition])
    val filePartition = partition.asInstanceOf[MergeFilePartition]

    val iter = filePartition.files.toIterator.map { files =>
      assert(files.forall(_.isInstanceOf[MergePartitionedFile]))
      files.map(f => f -> buildColumnarReader(f)).toSeq
    }.toSeq

    val mergeReader =
        new MergeParquetSingletonFilePartitionByBatchFile[InternalRow](iter)

    new MergeFilePartitionReader[InternalRow](
      Iterator(MergePartitionedFileReader( //filePartition.files.head,
        mergeReader))
    )
  }



  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] = ???


  def buildColumnarReader(partitionedFile: MergePartitionedFile): PartitionReader[ColumnarBatch]

}
