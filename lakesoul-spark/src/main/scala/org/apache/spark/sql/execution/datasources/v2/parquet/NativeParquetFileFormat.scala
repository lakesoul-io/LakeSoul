// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.datasources.v2.lakesoul.NativeLakeSoulFileFormat
import org.apache.spark.sql.internal.SQLConf

@deprecated("Use NativeLakeSoulFileFormat instead.", "3.0.0")
private[parquet] object NativeParquetFileFormat {
  def resolvePhysicalFormat(
      options: Map[String, String],
      job: Job,
      sqlConf: SQLConf): String = {
    NativeLakeSoulFileFormat.resolvePhysicalFormat(options, job, sqlConf)
  }

  def extensionForPhysicalFormat(physicalFormat: String): String = {
    NativeLakeSoulFileFormat.extensionForPhysicalFormat(physicalFormat)
  }
}

@deprecated("Use NativeLakeSoulFileFormat instead.", "3.0.0")
class NativeParquetFileFormat extends NativeLakeSoulFileFormat
