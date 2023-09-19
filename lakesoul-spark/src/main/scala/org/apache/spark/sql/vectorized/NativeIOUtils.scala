// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import com.dmetasoul.lakesoul.meta.DBUtil
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3a.S3AFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

import java.io.IOException
import scala.collection.JavaConverters._

class NativeIOUtils {
}

class NativeIOOptions(val s3Bucket: String,
                      val s3Ak: String,
                      val s3Sk: String,
                      val s3Endpoint: String,
                      val s3Region: String,
                      val fsUser: String,
                      val defaultFS: String
                     )

object NativeIOUtils{

  def asArrayColumnVector(vectorSchemaRoot: VectorSchemaRoot): Array[ColumnVector] = {
    asScalaIteratorConverter(vectorSchemaRoot.getFieldVectors.iterator())
      .asScala
      .toSeq
      .map(vector => {
        asColumnVector(vector)
      })
      .toArray
  }

  private def asArrowColumnVector(vector: ValueVector): org.apache.spark.sql.arrow.ArrowColumnVector = {
    new org.apache.spark.sql.arrow.ArrowColumnVector(vector)
  }

  private def asColumnVector(vector: ValueVector): ColumnVector = {
    asArrowColumnVector(vector).asInstanceOf[ColumnVector]
  }

  def getNativeIOOptions(taskAttemptContext: TaskAttemptContext, file: Path): NativeIOOptions = {
    var user: String = null
    val userConf = taskAttemptContext.getConfiguration.get("fs.hdfs.user")
    if (userConf != null) user = userConf
    var defaultFS = taskAttemptContext.getConfiguration.get("fs.defaultFS")
    if (defaultFS == null) defaultFS = taskAttemptContext.getConfiguration.get("fs.default.name")
    val fileSystem = file.getFileSystem(taskAttemptContext.getConfiguration)
    if (hasS3AFileSystemClass) {
      fileSystem match {
        case s3aFileSystem: S3AFileSystem =>
          val awsS3Bucket = s3aFileSystem.getBucket
          val s3aEndpoint = taskAttemptContext.getConfiguration.get("fs.s3a.endpoint")
          val s3aRegion = taskAttemptContext.getConfiguration.get("fs.s3a.endpoint.region")
          val s3aAccessKey = taskAttemptContext.getConfiguration.get("fs.s3a.access.key")
          val s3aSecretKey = taskAttemptContext.getConfiguration.get("fs.s3a.secret.key")
          return new NativeIOOptions(awsS3Bucket, s3aAccessKey, s3aSecretKey, s3aEndpoint, s3aRegion, user, defaultFS)
        case _ =>
      }
    }
    new NativeIOOptions(null, null, null, null, null, user, defaultFS)
  }

  def setNativeIOOptions(nativeIO: NativeIOBase, options: NativeIOOptions): Unit = {
    nativeIO.setObjectStoreOptions(
      options.s3Ak,
      options.s3Sk,
      options.s3Region,
      options.s3Bucket,
      options.s3Endpoint,
      options.fsUser,
      options.defaultFS
    )
  }

  def setParquetConfigurations(sparkSession: SparkSession, hadoopConf: Configuration, readDataSchema: StructType): Unit = {
    val readDataSchemaAsJson = readDataSchema.json
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(readDataSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    // Spark 3.3.2 introduced this config
    hadoopConf.set("spark.sql.legacy.parquet.nanosAsLong", "false")
  }

  private def hasS3AFileSystemClass: Boolean = {
    try {
      NativeIOUtils.getClass.getClassLoader.loadClass("org.apache.hadoop.fs.s3a.S3AFileSystem")
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  private def hasHdfsFileSystemClass: Boolean = {
    try {
      NativeIOUtils.getClass.getClassLoader.loadClass("org.apache.hadoop.hdfs.DistributedFileSystem")
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }

  def createAndSetTableDirPermission(path: Path, conf: Configuration): Unit = {
    // TODO: move these to native io
    // currently we only support setting owner and permission for HDFS.
    // S3 support will be added later
    if (!hasHdfsFileSystemClass) return

    val fs: FileSystem = path.getFileSystem(conf)
    if (fs.isInstanceOf[DistributedFileSystem]) {
      val userName = DBUtil.getUser
      val domain = DBUtil.getDomain
      if (userName == null || domain == "public") return
      val hdfs = fs.asInstanceOf[DistributedFileSystem]

      val nsDir = path.getParent
      if (!hdfs.exists(nsDir)) {
        hdfs.mkdirs(nsDir)
        hdfs.setOwner(nsDir, userName, domain)
        hdfs.setPermission(nsDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE))
      }
      if (hdfs.exists(path)) throw new IOException("Table directory already exists: " + path.toString)
      hdfs.mkdirs(path)
      hdfs.setOwner(path, userName, domain)
      hdfs.setPermission(path, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.NONE))
    }
  }
}
