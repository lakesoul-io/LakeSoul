/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.arrow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector.Bytes;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link ArrowReader} which read the underlying Arrow format data as {@link RowData}. */
@Internal
public final class ArrowReader {

    private static final Logger LOG = LoggerFactory.getLogger(ArrowReader.class);

    /**
     * An array of vectors which are responsible for the deserialization of each column of the rows.
     */
    private final ColumnVector[] columnVectors;

    /** Reusable row used to hold the deserialized result. */
    private final ColumnarRowData reuseRow;

    public ArrowReader(ColumnVector[] columnVectors) {
        this.columnVectors = Preconditions.checkNotNull(columnVectors);
        this.reuseRow = new ColumnarRowData();
        this.reuseRow.setVectorizedColumnBatch(new VectorizedColumnBatch(columnVectors));
    }

    /** Gets the column vectors. */
    public ColumnVector[] getColumnVectors() {
        return columnVectors;
    }

    /** Read the specified row from underlying Arrow format data. */
    public RowData read(int rowId) {
        reuseRow.setRowId(rowId);
        if (LOG.isDebugEnabled()) {
            StringBuilder sb =
                    new StringBuilder("rowId=")
                            .append(rowId)
                            .append(",arity=")
                            .append(columnVectors.length);
            for (int i = 0; i < columnVectors.length; i++) {
                ColumnVector vector = columnVectors[i];
                sb.append(" [")
                        .append(i)
                        .append(':')
                        .append(vector.getClass().getSimpleName())
                        .append(",null=")
                        .append(vector.isNullAt(rowId));
                if (vector instanceof BytesColumnVector) {
                    try {
                        Bytes bytes = ((BytesColumnVector) vector).getBytes(rowId);
                        sb.append(",bytes=")
                                .append(
                                        bytes == null
                                                ? "null"
                                                : "data="
                                                        + (bytes.data == null
                                                                ? "null"
                                                                : bytes.data.length)
                                                        + ",offset="
                                                        + bytes.offset
                                                        + ",len="
                                                        + bytes.len);
                    } catch (Throwable t) {
                        sb.append(",bytesErr=").append(t);
                    }
                }
                sb.append(']');
            }
            LOG.debug("ArrowReader.read {}", sb);
        }
        return reuseRow;
    }
}
