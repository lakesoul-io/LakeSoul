package org.apache.spark.sql.execution.datasources.v2.parquet.Native

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native.NativeMergeLogic
import org.apache.spark.sql.vectorized.ColumnarBatch

case class NativeFilePartitionReader[T](filesInfo: Seq[(Array[MergePartitionedFile], PartitionReader[ColumnarBatch])])
  extends PartitionReader[InternalRow] with Logging {
  logInfo("[Debug][huazeng]on initialize "+filesInfo.toString())

  val partitionedFilesItr: Iterator[(Array[MergePartitionedFile], PartitionReader[ColumnarBatch])] = filesInfo.iterator
  var nativeMergeLogic:NativeMergeLogic = _

  override def next(): Boolean = {
    logInfo("[Debug][huazeng]on next")
    if (nativeMergeLogic == null) {
      if (partitionedFilesItr.hasNext) {
        val nextFiles = partitionedFilesItr.next()
        if (nextFiles._1.isEmpty) {
          return false
        } else {
          nativeMergeLogic = new NativeMergeLogic(nextFiles)
        }
      } else {
        return false
      }
    }
    if (nativeMergeLogic.alive){
      return true
    } else if (partitionedFilesItr.hasNext) {
      nativeMergeLogic.closeReadFileReader()
      nativeMergeLogic = new NativeMergeLogic(partitionedFilesItr.next())
      nativeMergeLogic.alive
    }
    false
  }

  override def get(): InternalRow = {
    logInfo("[Debug][huazeng]on get")
    nativeMergeLogic.getRow
  }

  override def close(): Unit = {
    logInfo("[Debug][huazeng]on close")
    if (filesInfo.nonEmpty) {
      filesInfo.foreach(_._2.close())
    }
  }
}
