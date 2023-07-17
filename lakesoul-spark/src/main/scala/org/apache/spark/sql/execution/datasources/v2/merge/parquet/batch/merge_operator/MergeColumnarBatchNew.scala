// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeOperatorColumnarBatchRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

/**
  * Construct a column batch for merged result.
  *
  * @param columns        ordered column vectors of all file
  * @param mergeOps       merge operators
  * @param indexTypeArray result schema index and type
  */
class MergeColumnarBatchNew(columns: Array[ColumnVector],
                            mergeOps: Seq[MergeOperator[Any]],
                            indexTypeArray: Seq[FieldIndex]) extends AutoCloseable {

  val row = new MergeOperatorColumnarBatchRow(columns, mergeOps, indexTypeArray)

  def getRow(resultIndex: Seq[Seq[MergeColumnIndex]]): InternalRow = {
    row.idMix = resultIndex
    row.mergeValues()
    row
  }

  def getMergeRow(resultIndex: Seq[Seq[MergeColumnIndex]]): MergeOperatorColumnarBatchRow = {
    row.idMix = resultIndex
    row.mergeValues()
    row
  }

  override def close(): Unit = {
    for (c <- columns) {
      c.close()
    }
  }


}
