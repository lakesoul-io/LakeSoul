// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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

