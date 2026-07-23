// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.v2.lakesoul.NativeLakeSoulOutputWriter
import org.apache.spark.sql.types.StructType

@deprecated("Use NativeLakeSoulOutputWriter instead.", "3.0.0")
class NativeParquetOutputWriter(path: String,
                                dataSchema: StructType,
                                timeZoneId: String,
                                context: TaskAttemptContext,
                                physicalFormat: String = "vortex-compact")
  extends NativeLakeSoulOutputWriter(path, dataSchema, timeZoneId, context, physicalFormat)
