// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.lakesoul

import java.util.Locale

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._

private[v2] object NativeLakeSoulFileFormat {
  def resolvePhysicalFormat(
      options: Map[String, String],
      job: Job,
      sqlConf: SQLConf): String = {
    options
      .get(LakeSoulOptions.FILE_FORMAT)
      .orElse(Option(job.getConfiguration.get(LakeSoulOptions.FILE_FORMAT)))
      .getOrElse(sqlConf.getConf(LakeSoulSQLConf.NATIVE_IO_PHYSICAL_FORMAT))
      .toLowerCase(Locale.ROOT)
  }

  def extensionForPhysicalFormat(physicalFormat: String): String = {
    physicalFormat.toLowerCase(Locale.ROOT) match {
      case "vortex" | "vortex-compact" => ".vortex"
      case _ => ".parquet"
    }
  }
}

class NativeLakeSoulFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {

    val timeZoneId = options
      .getOrElse(DateTimeUtils.TIMEZONE_OPTION, sparkSession.sessionState.conf.sessionLocalTimeZone)
    val physicalFormat =
      NativeLakeSoulFileFormat.resolvePhysicalFormat(options, job, sparkSession.sessionState.conf)
    // vortex is only supported when native io is enabled
    val extension =
      NativeLakeSoulFileFormat.extensionForPhysicalFormat(physicalFormat)

    if (options.getOrElse("isNative", "true").toBoolean) {
      new OutputWriterFactory {
        override def newInstance(
                                  path: String,
                                  dataSchema: StructType,
                                  context: TaskAttemptContext): OutputWriter = {
          logInfo(s"Use native columnar $physicalFormat writer for $path")
          new NativeLakeSoulColumnarOutputWriter(path, dataSchema, timeZoneId, context, physicalFormat)
        }

        override def getFileExtension(context: TaskAttemptContext): String = {
          CodecConfig.from(context).getCodec.getExtension + extension
        }
      }
    } else {
      new OutputWriterFactory {
        override def newInstance(
                                  path: String,
                                  dataSchema: StructType,
                                  context: TaskAttemptContext): OutputWriter = {
          logInfo(s"Use native $physicalFormat writer for $path")
          new NativeLakeSoulOutputWriter(path, dataSchema, timeZoneId, context, physicalFormat)
        }

        override def getFileExtension(context: TaskAttemptContext): String = {
          CodecConfig.from(context).getCodec.getExtension + extension
        }
      }
    }
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            parameters: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {
    ParquetUtils.inferSchema(sparkSession, parameters, files)
  }

  override def shortName(): String = "parquet"

  override def toString: String = "LakeSoulNative"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean =
    other.isInstanceOf[NativeLakeSoulFileFormat] || other.isInstanceOf[ParquetFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    case st: StructType => st.forall { f => supportDataType(f.dataType) }

    case ArrayType(elementType, _) => supportDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportDataType(keyType) && supportDataType(valueType)

    case udt: UserDefinedType[_] => supportDataType(udt.sqlType)

    case _ => false
  }
}
