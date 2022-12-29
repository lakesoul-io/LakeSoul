/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.tool;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class RowDataProjection implements Serializable {
  private static final long serialVersionUID = 1L;

  private final RowData.FieldGetter[] fieldGetters;

  private RowDataProjection(LogicalType[] types, int[] positions) {
    this.fieldGetters = new RowData.FieldGetter[types.length];
    for (int i = 0; i < types.length; i++) {
      final LogicalType type = types[i];
      final int pos = positions[i];
      this.fieldGetters[i] = RowData.createFieldGetter(type, pos);
    }
  }

  public static RowDataProjection instance(RowType rowType, int[] positions) {
    final LogicalType[] types = rowType.getChildren().toArray(new LogicalType[0]);
    return new RowDataProjection(types, positions);
  }

  public static RowDataProjection instance(LogicalType[] types, int[] positions) {
    return new RowDataProjection(types, positions);
  }

  public RowData project(RowData rowData) {
    GenericRowData genericRowData = new GenericRowData(this.fieldGetters.length);
    for (int i = 0; i < this.fieldGetters.length; i++) {
      final Object val = this.fieldGetters[i].getFieldOrNull(rowData);
      genericRowData.setField(i, val);
    }
    return genericRowData;
  }

}

