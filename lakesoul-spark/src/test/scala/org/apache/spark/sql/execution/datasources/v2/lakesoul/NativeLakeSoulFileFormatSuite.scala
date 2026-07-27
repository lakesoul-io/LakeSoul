// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.lakesoul

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.internal.SQLConf
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NativeLakeSoulFileFormatSuite extends AnyFunSuite {

  test("default physical format selects vortex extension") {
    val job = Job.getInstance()

    assert(fileExtension(job, Map.empty).endsWith(".vortex"))
  }

  test("file_format option selects vortex extension case-insensitively") {
    val job = Job.getInstance()

    assert(fileExtension(job, Map(LakeSoulOptions.FILE_FORMAT -> "VORTEX")).endsWith(".vortex"))
  }

  test("file_format option selects vortex compact extension") {
    val job = Job.getInstance()

    assert(fileExtension(job, Map(LakeSoulOptions.FILE_FORMAT -> "vortex-compact")).endsWith(".vortex"))
  }

  test("file_format option keeps parquet extension") {
    val job = Job.getInstance()

    assert(fileExtension(job, Map(LakeSoulOptions.FILE_FORMAT -> "parquet")).endsWith(".parquet"))
  }

  test("file_format table configuration selects vortex extension") {
    val job = Job.getInstance()
    job.getConfiguration.set(LakeSoulOptions.FILE_FORMAT, "vortex")

    assert(fileExtension(job, Map.empty).endsWith(".vortex"))
  }

  private def fileExtension(job: Job, options: Map[String, String]): String = {
    val sqlConf = new SQLConf
    NativeLakeSoulFileFormat.extensionForPhysicalFormat(
      NativeLakeSoulFileFormat.resolvePhysicalFormat(options, job, sqlConf))
  }
}
