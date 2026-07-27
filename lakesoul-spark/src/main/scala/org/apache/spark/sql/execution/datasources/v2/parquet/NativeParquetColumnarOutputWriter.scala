// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.v2.lakesoul.NativeLakeSoulColumnarOutputWriterSupport
import org.apache.spark.sql.types.StructType

@deprecated("Use NativeLakeSoulColumnarOutputWriter instead.", "3.0.0")
class NativeParquetColumnarOutputWriter(path: String, dataSchema: StructType, timeZoneId: String,
                                        context: TaskAttemptContext, physicalFormat: String = "vortex-compact")
  extends NativeParquetOutputWriter(path, dataSchema, timeZoneId, context, physicalFormat)
    with NativeLakeSoulColumnarOutputWriterSupport
