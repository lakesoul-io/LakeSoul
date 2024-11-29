// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetUtils}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._

class NativeParquetFileFormat extends FileFormat
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

    if (options.getOrElse("isCompaction", "false").toBoolean &&
      !options.getOrElse("isCDC", "false").toBoolean &&
      !options.getOrElse("isBucketNumChanged", "false").toBoolean &&
      options.contains("staticBucketId")
    ) {
      new OutputWriterFactory {
        override def newInstance(
                                  path: String,
                                  dataSchema: StructType,
                                  context: TaskAttemptContext): OutputWriter = {
          new NativeParquetCompactionColumnarOutputWriter(path, dataSchema, timeZoneId, context)
        }

        override def getFileExtension(context: TaskAttemptContext): String = {
          CodecConfig.from(context).getCodec.getExtension + ".parquet"
        }
      }
    } else {
      new OutputWriterFactory {
        override def newInstance(
                                  path: String,
                                  dataSchema: StructType,
                                  context: TaskAttemptContext): OutputWriter = {
          new NativeParquetOutputWriter(path, dataSchema, timeZoneId, context)
        }

        override def getFileExtension(context: TaskAttemptContext): String = {
          CodecConfig.from(context).getCodec.getExtension + ".parquet"
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

  override def toString: String = "Parquet"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[ParquetFileFormat]

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
