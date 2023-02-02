package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.{Job, OutputCommitter, TaskAttemptContext}
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.apache.parquet.hadoop.ParquetOutputFormat.JobSummaryLevel
import org.apache.parquet.hadoop.codec.CodecConfig
import org.apache.parquet.hadoop.util.ContextUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions, ParquetUtils, ParquetWriteSupport}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{ArrayType, AtomicType, DataType, MapType, StructType, UserDefinedType}

class NativeParquetFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {

    val timeZoneId = options.get(DateTimeUtils.TIMEZONE_OPTION)
      .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone)

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
