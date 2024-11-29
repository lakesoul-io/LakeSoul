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
class DelayedCopyCommitProtocol(srcFiles: Seq[DataFileInfo],
                                jobId: String,
                                dstPath: String,
                                randomPrefixLength: Option[Int])
  extends DelayedCommitProtocol(jobId, dstPath, randomPrefixLength)
    with Serializable with Logging {

  @transient private var addedFileInfo: ArrayBuffer[DataFileInfo] = _
  @transient private var pathToFileInfo: Map[String, Seq[DataFileInfo]] = _

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    addedFileInfo = new ArrayBuffer[DataFileInfo]
    pathToFileInfo = srcFiles.groupBy(_.path)
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    addedFileInfo += pathToFileInfo(ext).head
    ext
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    if (addedFileInfo.nonEmpty) {
      val fs = new Path(addedFileInfo.head.path).getFileSystem(taskContext.getConfiguration)
      val statuses = addedFileInfo.map { srcFile =>
        val (srcCompactDir, srcBasePath) = splitCompactFilePath(srcFile.path)
        val dstFile = new Path(dstPath, srcBasePath)
        FileUtil.copy(fs, new Path(srcFile.path), fs, new Path(dstPath, srcBasePath), false, taskContext.getConfiguration)
        val status = fs.getFileStatus(dstFile)
        DataFileInfo(srcFile.range_partitions, fs.makeQualified(dstFile).toString, "add", status.getLen, status.getModificationTime)
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
