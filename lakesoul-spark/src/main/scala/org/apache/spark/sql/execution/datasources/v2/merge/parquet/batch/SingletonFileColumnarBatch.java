// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch;


import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class SingletonFileColumnarBatch implements AutoCloseable {

    private final ColumnVector[] columns;

    // Staging row returned from `getRow`.
    private final SingletonBatchRow row;

    public SingletonFileColumnarBatch(ColumnVector[] columns) {
        this.columns = columns;
        this.row = new SingletonBatchRow(columns);
    }

    public void updateBatch(ColumnarBatch mergeColumns, int[] updateIndex) {
        for (int i = 0; i < updateIndex.length; i++) {
            columns[updateIndex[i]] = mergeColumns.column(i);
        }
    }

    public InternalRow getRow(Integer rowId) {
        row.rowId = rowId;
        return row;
    }

    @Override
    public void close() {
        for (ColumnVector c : columns) {
            c.close();
        }
    }
}