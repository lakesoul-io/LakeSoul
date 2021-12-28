package org.apache.spark.sql.lakesoul.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.io.{FileNotFoundException, IOException}
import java.net.URI
import java.util.Locale
import scala.util.Random
import scala.util.control.NonFatal

object FileOperation extends Logging {

  /**
    * Create an absolute path from `child` using the `basePath` if the child is a relative path.
    * Return `child` if it is an absolute path.
    *
    * @param basePath Base path to prepend to `child` if child is a relative path.
    *                 Note: It is assumed that the basePath do not have any escaped characters and
    *                 is directly readable by Hadoop APIs.
    * @param child    Child path to append to `basePath` if child is a relative path.
    *                 Note: t is assumed that the child is escaped, that is, all special chars that
    *                 need escaping by URI standards are already escaped.
    * @return Absolute path without escaped chars that is directly readable by Hadoop APIs.
    */
  def absolutePath(basePath: String, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      val merged = new Path(basePath, p)
      // URI resolution strips the final `/` in `p` if it exists
      val mergedUri = merged.toUri.toString
      if (child.endsWith("/") && !mergedUri.endsWith("/")) {
        new Path(new URI(mergedUri + "/"))
      } else {
        merged
      }
    }
  }

  /**
    * Given a path `child`:
    *   1. Returns `child` if the path is already relative
    *   2. Tries relativizing `child` with respect to `basePath`
    * a) If the `child` doesn't live within the same base path, returns `child` as is
    * b) If `child` lives in a different FileSystem, throws an exception
    * Note that `child` may physically be pointing to a path within `basePath`, but may logically
    * belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
    */
  def tryRelativizePath(fs: FileSystem, basePath: Path, child: Path): Path = {
    // Child Paths can be absolute and use a separate fs
    val childUri = child.toUri
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme s3://, wasbs:// or adls://.
               |This is a bug that has existed before DBR 5.0. To fix this issue, please upgrade
               |your writer jobs to DBR 5.0
             """.stripMargin)
      }
    } else {
      child
    }
  }

  /** Check if the thrown exception is a throttling error. */
  private def isThrottlingError(t: Throwable): Boolean = {
    Option(t.getMessage).exists(_.toLowerCase(Locale.ROOT).contains("slow down"))
  }

  private def randomBackoff(opName: String,
                            t: Throwable,
                            base: Int = 100,
                            jitter: Int = 1000): Unit = {
    val sleepTime = Random.nextInt(jitter) + base
    logWarning(s"Sleeping for $sleepTime ms to rate limit $opName", t)
    Thread.sleep(sleepTime)
  }

  /**
    * The default filter for hidden files. Files names beginning with _ or . are considered hidden.
    *
    * @param fileName
    * @return true if the file is hidden
    */
  def defaultHiddenFileFilter(fileName: String): Boolean = {
    fileName.startsWith("_") || fileName.startsWith(".")
  }

  /**
    * Returns all the levels of sub directories that `path` has with respect to `base`. For example:
    * getAllSubDirectories("/base", "/base/a/b/c") =>
    * (Iterator("/base/a", "/base/a/b"), "/base/a/b/c")
    */
  def getAllSubDirectories(base: String, path: String): (Iterator[String], String) = {
    val baseSplits = base.split(Path.SEPARATOR)
    val pathSplits = path.split(Path.SEPARATOR).drop(baseSplits.length)
    val it = Iterator.tabulate(pathSplits.length - 1) { i =>
      (baseSplits ++ pathSplits.take(i + 1)).mkString(Path.SEPARATOR)
    }
    (it, path)
  }


}
