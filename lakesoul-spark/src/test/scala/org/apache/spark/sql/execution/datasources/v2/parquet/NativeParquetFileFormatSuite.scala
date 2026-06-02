// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.mapreduce.{Job, JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NativeParquetFileFormatSuite extends QueryTest with SharedSparkSession {

  test("file_format option selects vortex extension case-insensitively") {
    val job = Job.getInstance()

    assert(fileExtension(job, Map(LakeSoulOptions.FILE_FORMAT -> "VORTEX")).endsWith(".vortex"))
  }

  test("file_format table configuration selects vortex extension") {
    val job = Job.getInstance()
    job.getConfiguration.set(LakeSoulOptions.FILE_FORMAT, "vortex")

    assert(fileExtension(job, Map.empty).endsWith(".vortex"))
  }

  private def fileExtension(job: Job, options: Map[String, String]): String = {
    val dataSchema = new StructType().add("id", IntegerType)
    val writerFactory = new NativeParquetFileFormat()
      .prepareWrite(spark, job, options, dataSchema)
    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val context = new TaskAttemptContextImpl(job.getConfiguration, attemptId)

    writerFactory.getFileExtension(context)
  }
}
