// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge

import java.io.{FileNotFoundException, IOException}

import org.apache.parquet.io.ParquetDecodingException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.internal.SQLConf

class MergeFilePartitionReader[T](readers: Iterator[MergePartitionedFileReader[T]])
  extends PartitionReader[T] with Logging {
  private var currentReader: MergePartitionedFileReader[T] = null

  private val sqlConf = SQLConf.get

  private def ignoreMissingFiles = sqlConf.ignoreMissingFiles

  private def ignoreCorruptFiles = sqlConf.ignoreCorruptFiles

  override def next(): Boolean = {
    if (currentReader == null) {
      if (readers.hasNext) {
        try {
          currentReader = getNextReader()
        } catch {
          case e: FileNotFoundException if ignoreMissingFiles =>
            logWarning(s"Skipped missing file.", e)
            currentReader = null
          // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
          case e: FileNotFoundException if !ignoreMissingFiles =>
            throw new FileNotFoundException(
              e.getMessage + "\n" +
                "It is possible the underlying files have been updated. " +
                "You can explicitly invalidate the cache in Spark by " +
                "recreating the Dataset/DataFrame involved.")
          case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest of the content in the corrupted file.", e)
            currentReader = null
        }
      } else {
        return false
      }
    }

    // In PartitionReader.next(), the current reader proceeds to next record.
    // It might throw RuntimeException/IOException and Spark should handle these exceptions.
    val hasNext = try {
      currentReader != null && currentReader.next()
    } catch {
      case e: SchemaColumnConvertNotSupportedException =>
        val message = "Parquet column cannot be converted in " +
          s"file. Column: ${e.getColumn}, " +
          s"Expected: ${e.getLogicalType}, Found: ${e.getPhysicalType}"
        throw new QueryExecutionException(message, e)
      case e: ParquetDecodingException =>
        if (e.getMessage.contains("Can not read value at")) {
          val message = "Encounter error while reading parquet files. " +
            "One possible cause: Parquet column cannot be converted in the " +
            "corresponding files. Details: "
          throw new QueryExecutionException(message, e)
        }
        throw e
      case e@(_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
        logWarning(
          s"Skipped the rest of the content in the corrupted file: $currentReader", e)
        false
    }
    if (hasNext) {
      true
    } else {
      close()
      currentReader = null
      next()
    }
  }

  override def get(): T = currentReader.get()

  override def close(): Unit = {
    if (currentReader != null) {
      currentReader.close()
    }
    InputFileBlockHolder.unset()
  }

  private def getNextReader(): MergePartitionedFileReader[T] = {
    val reader = readers.next()
    logInfo(s"Reading file $reader")
    reader
  }
}


private[v2] case class MergePartitionedFileReader[T](reader: PartitionReader[T]) extends PartitionReader[T] {
  override def next(): Boolean = reader.next()

  override def get(): T = reader.get()

  override def close(): Unit = reader.close()

}