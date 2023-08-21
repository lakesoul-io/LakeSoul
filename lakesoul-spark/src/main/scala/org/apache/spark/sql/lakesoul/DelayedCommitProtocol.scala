// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.MetaUtils

import java.net.URI
import java.util.UUID
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, DateFormatter, PartitionUtils, TimestampFormatter}
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Writes out the files to `path` and returns a list of them in `addedStatuses`.
  */
class DelayedCommitProtocol(jobId: String,
                            path: String,
                            randomPrefixLength: Option[Int])
  extends FileCommitProtocol
    with Serializable with Logging {

  // Track the list of files added by a task, only used on the executors.
  @transient private var addedFiles: ArrayBuffer[(List[(String, String)], String)] = _
  @transient val addedStatuses = new ArrayBuffer[DataFileInfo]

  val timestampPartitionPattern = "yyyy-MM-dd HH:mm:ss[.S]"


  override def setupJob(jobContext: JobContext): Unit = {

  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    val fileStatuses = taskCommits.flatMap(_.obj.asInstanceOf[Seq[DataFileInfo]]).toArray
    addedStatuses ++= fileStatuses
  }

  override def abortJob(jobContext: JobContext): Unit = {
    // TODO: Best effort cleanup
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    addedFiles = new ArrayBuffer[(List[(String, String)], String)]
  }

  protected def getFileName(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    val uuid = UUID.randomUUID.toString
    f"part-$split%05d-$uuid$ext"
  }

  protected def parsePartitions(dir: String): List[(String, String)] = {
    // TODO: timezones?
    // TODO: enable validatePartitionColumns?
    val dateFormatter = DateFormatter()
    val timestampFormatter =
      TimestampFormatter(timestampPartitionPattern, java.util.TimeZone.getDefault)
    val parsedPartition =
      PartitionUtils
        .parsePartition(
          new Path(dir),
          typeInference = false,
          Set.empty,
          Map.empty,
          validatePartitionColumns = false,
          java.util.TimeZone.getDefault,
          dateFormatter,
          timestampFormatter)
        ._1
        .get
    parsedPartition.columnNames.zip(parsedPartition.literals.map(l => Cast(l, StringType).eval()).map(Option(_).map(_.toString).orNull)).toList
  }

  /** Generates a string created of `randomPrefixLength` alphanumeric characters. */
  private def getRandomPrefix(numChars: Int): String = {
    Random.alphanumeric.take(numChars).mkString
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    val filename = getFileName(taskContext, ext)
    val partitionValues = dir.map(parsePartitions).getOrElse(List.empty[(String, String)])
    val relativePath = randomPrefixLength.map { prefixLength =>
      getRandomPrefix(prefixLength) // Generate a random prefix as a first choice
    }.orElse {
      dir // or else write into the partition directory if it is partitioned
    }.map { subDir =>
      new Path(subDir, filename)
    }.getOrElse(new Path(filename)) // or directly write out to the output path

    val absolutePath = new Path(path, relativePath).toUri.toString
    //returns the absolute path to the file
    addedFiles.append((partitionValues, absolutePath))
    absolutePath
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
    throw new UnsupportedOperationException(
      s"$this does not support adding files with an absolute path")
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {

    if (addedFiles.nonEmpty) {
      val fs = new Path(path, addedFiles.head._2).getFileSystem(taskContext.getConfiguration)
      val statuses: Seq[DataFileInfo] = addedFiles.map { f =>

        val filePath = new Path(new URI(f._2))
        val stat = fs.getFileStatus(filePath)
        DataFileInfo(MetaUtils.getPartitionKeyFromList(f._1),fs.makeQualified(filePath).toString, "add", stat.getLen, stat.getModificationTime)
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
