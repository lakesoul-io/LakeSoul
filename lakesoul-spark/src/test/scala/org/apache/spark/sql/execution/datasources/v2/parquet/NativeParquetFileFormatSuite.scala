// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.mapreduce.{Job, JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NativeParquetFileFormatSuite extends QueryTest with SharedSparkSession {

  test("vortex physical format selects vortex extension case-insensitively") {
    withSQLConf(LakeSoulSQLConf.NATIVE_IO_PHYSICAL_FORMAT.key -> "VORTEX") {
      val job = Job.getInstance()
      val dataSchema = new StructType().add("id", IntegerType)
      val writerFactory = new NativeParquetFileFormat()
        .prepareWrite(spark, job, Map.empty, dataSchema)
      val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
      val context = new TaskAttemptContextImpl(job.getConfiguration, attemptId)

      assert(writerFactory.getFileExtension(context).endsWith(".vortex"))
    }
  }
}
