package org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeLogic
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util.Iterator

class NativeMergeLogic(filesInfo: (Array[MergePartitionedFile], PartitionReader[ColumnarBatch])) extends MergeLogic {
  var rowId: Int = _

  val fileItr = filesInfo._2

  var columnarBatch: ColumnarBatch = _

  var rowItr: Iterator[InternalRow]= _

  protected var readerAlive: Boolean = true

  protected def setAlive(alive:Boolean) {readerAlive = alive}

  def alive: Boolean = readerAlive



  def getRow: InternalRow = {
    if (columnarBatch==null) {
      if (fileItr.next()) {
        columnarBatch = fileItr.get()
        rowItr = columnarBatch.rowIterator()
      } else {
        setAlive(false)
        return null
      }
    }
    if (rowItr.hasNext) {
      return rowItr.next()
    } else {
      setAlive(false)
      return null
    }
  }

  /** The file readers have been read should be closed at once. */
  override def closeReadFileReader(): Unit = ???

}
