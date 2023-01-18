package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.arrow.lakesoul.io.NativeIOWriter
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.NativeIOUtils

class NativeParquetOutputWriter(val path: String, dataSchema: StructType, timeZoneId: String, context: TaskAttemptContext) extends OutputWriter {

  private var recordCount = 0L

  val nativeIOWriter = new NativeIOWriter(NativeIOUtils.convertStructTypeToArrowJson(dataSchema, timeZoneId))
  nativeIOWriter.addFile(path)
  nativeIOWriter.initializeWriter()

  val arrowSchema = ArrowUtils.toArrowSchema(dataSchema, timeZoneId)

  val allocator =
    ArrowMemoryUtils.rootAllocator.newChildAllocator("toBatchIterator", 0, Long.MaxValue)
  val root = VectorSchemaRoot.create(arrowSchema, allocator)

  val recordWriter: ArrowWriter = ArrowWriter.create(root)

  override def write(row: InternalRow): Unit = {
    recordWriter.write(row)
  }

//  def checkBlockSizeReached() {
//    if (this.recordCount >= this.recordCountForNextMemCheck) {
//      val memSize = this.columnStore.getBufferedSize
//      val recordSize = memSize / this.recordCount
//      if (memSize > this.nextRowGroupSize - 2L * recordSize) {
//        LOG.debug("mem size {} > {}: flushing {} records to disk.", Array[AnyRef](memSize, this.nextRowGroupSize, this.recordCount))
//        this.flushRowGroupToStore()
//        this.initStore()
//        this.recordCountForNextMemCheck = Math.min(Math.max(this.props.getMinRowCountForPageSizeCheck.toLong, this.recordCount / 2L), this.props.getMaxRowCountForPageSizeCheck.toLong)
//        this.lastRowGroupEndPos = this.parquetFileWriter.getPos
//      } else {
//        this.recordCountForNextMemCheck = Math.min(Math.max(this.props.getMinRowCountForPageSizeCheck.toLong, (this.recordCount + (this.nextRowGroupSize.toFloat / recordSize.toFloat).toLong) / 2L), this.recordCount + this.props.getMaxRowCountForPageSizeCheck.toLong)
//        LOG.debug("Checked mem at {} will check again at: {}", this.recordCount, this.recordCountForNextMemCheck)
//      }
//    }
//  }

  override def close(): Unit = {

    recordWriter.finish()

    nativeIOWriter.write(root)
    nativeIOWriter.close()

    recordWriter.reset()
    root.close()
    allocator.close()

  }
}
