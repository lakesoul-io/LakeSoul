package org.apache.spark.sql.lakesoul.utils

import java.util.Objects

import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}

/** A serializable variant of HDFS's FileStatus. */
case class SerializableFileStatus(path: String,
                                  length: Long,
                                  isDir: Boolean,
                                  modificationTime: Long) {

  // Important note! This is very expensive to compute, but we don't want to cache it
  // as a `val` because Paths internally contain URIs and therefore consume lots of memory.
  def getPath: Path = new Path(path)

  def getLen: Long = length

  def getModificationTime: Long = modificationTime

  def isDirectory: Boolean = isDir

  def toFileStatus: FileStatus = {
    new LocatedFileStatus(
      new FileStatus(length, isDir, 0, 0, modificationTime, new Path(path)),
      null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: SerializableFileStatus =>
      // We only compare the paths to stay consistent with FileStatus.equals.
      Objects.equals(path, other.path)
    case _ => false
  }

  override def hashCode(): Int = {
    // We only use the path to stay consistent with FileStatus.hashCode.
    Objects.hashCode(path)
  }
}

object SerializableFileStatus {
  def fromStatus(status: FileStatus): SerializableFileStatus = {
    SerializableFileStatus(
      Option(status.getPath).map(_.toString).orNull,
      status.getLen,
      status.isDirectory,
      status.getModificationTime)
  }

  val EMPTY: SerializableFileStatus = fromStatus(new FileStatus())
}
