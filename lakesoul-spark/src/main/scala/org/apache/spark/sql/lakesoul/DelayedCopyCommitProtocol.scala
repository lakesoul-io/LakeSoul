// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{DataFileInfo, MetaUtils}
import com.dmetasoul.lakesoul.spark.clean.CleanOldCompaction.splitCompactFilePath
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.lakesoul.utils.{DateFormatter, PartitionUtils, TimestampFormatter}
import org.apache.spark.sql.types.StringType

import java.net.URI
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Writes out the files to `path` and returns a list of them in `addedStatuses`.
  */
class DelayedCopyCommitProtocol(jobId: String,
                                dstPath: String,
                                randomPrefixLength: Option[Int])
  extends DelayedCommitProtocol(jobId, dstPath, randomPrefixLength)
    with Serializable with Logging {

  @transient private var copyFiles: ArrayBuffer[(String, String)] = _

  override def setupJob(jobContext: JobContext): Unit = {

  }

  override def abortJob(jobContext: JobContext): Unit = {
    // TODO: Best effort cleanup
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    copyFiles = new ArrayBuffer[(String, String)]
  }


  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val (srcCompactDir, srcBasePath) = splitCompactFilePath(ext)
    copyFiles += dir.getOrElse("-5") -> ext
    new Path(dstPath, srcBasePath).toString
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {

    if (copyFiles.nonEmpty) {
      val fs = new Path(copyFiles.head._2).getFileSystem(taskContext.getConfiguration)
      val statuses = copyFiles.map { f =>
        val (partitionDesc, srcPath) = f
        val (srcCompactDir, srcBasePath) = splitCompactFilePath(srcPath)
        val dstFile = new Path(dstPath, srcBasePath)
        FileUtil.copy(fs, new Path(srcPath), fs, new Path(dstPath, srcBasePath), false, taskContext.getConfiguration)
        val status = fs.getFileStatus(dstFile)
        DataFileInfo(partitionDesc, fs.makeQualified(dstFile).toString, "add", status.getLen, status.getModificationTime)
      }

      new TaskCommitMessage(statuses)
    } else {
      new TaskCommitMessage(Nil)
    }
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    // TODO: we can also try delete the addedFiles as a best-effort cleanup.
  }
}
