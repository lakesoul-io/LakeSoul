/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionComputer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CdcPartitionComputer implements PartitionComputer<RowData> {

  private static final long serialVersionUID = 1L;

  protected String defaultPartValue;
  protected String[] partitionColumns;
  protected int[] partitionIndexes;
  protected LogicalType[] partitionTypes;
  protected RowData.FieldGetter[] partitionFieldGetters;

  private final int[] nonPartitionIndexes;
  private final LogicalType[] nonPartitionTypes;
  protected RowData.FieldGetter[] nonPartitionFieldGetters;
  private final Boolean isCdc;
  private transient GenericRowData reuseRow;

  public CdcPartitionComputer(
          String defaultPartValue,
          String[] columnNames,
          RowType rowType,
          String[] partitionColumns, Boolean isCdc) {
    this(defaultPartValue, columnNames,
            rowType.getChildren(),
            partitionColumns, isCdc);
  }

  public CdcPartitionComputer(
          String defaultPartValue,
          String[] columnNames,
          DataType[] columnTypes,
          String[] partitionColumns, Boolean isCdc) {
    this(defaultPartValue, columnNames,
            Arrays.stream(columnTypes)
                    .map(DataType::getLogicalType)
                    .collect(Collectors.toList()),
            partitionColumns, isCdc);
  }

  public CdcPartitionComputer(
          String defaultPartValue,
          String[] columnNames,
          List<LogicalType> columnTypeList,
          String[] partitionColumns, Boolean isCdc) {
    this.defaultPartValue = defaultPartValue;
    this.isCdc = isCdc;
    List<String> columnList = Arrays.asList(columnNames);

    this.partitionColumns = partitionColumns;
    this.partitionIndexes =
            Arrays.stream(this.partitionColumns).mapToInt(columnList::indexOf).toArray();
    this.partitionTypes =
            Arrays.stream(partitionIndexes)
                    .mapToObj(columnTypeList::get)
                    .toArray(LogicalType[]::new);
    this.partitionFieldGetters =
            IntStream.range(0, partitionTypes.length)
                    .mapToObj(
                            i ->
                                    RowData.createFieldGetter(
                                            partitionTypes[i], partitionIndexes[i]))
                    .toArray(RowData.FieldGetter[]::new);

    List<Integer> partitionIndexList =
            Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
    this.nonPartitionIndexes =
            IntStream.range(0, columnNames.length)
                    .filter(c -> !partitionIndexList.contains(c))
                    .toArray();
    this.nonPartitionTypes =
            Arrays.stream(nonPartitionIndexes)
                    .mapToObj(columnTypeList::get)
                    .toArray(LogicalType[]::new);
    this.nonPartitionFieldGetters =
            IntStream.range(0, nonPartitionTypes.length)
                    .mapToObj(
                            i ->
                                    RowData.createFieldGetter(
                                            nonPartitionTypes[i], nonPartitionIndexes[i]))
                    .toArray(RowData.FieldGetter[]::new);
  }

  @Override
  public LinkedHashMap<String, String> generatePartValues(RowData in) {
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

    for (int i = 0; i < partitionIndexes.length; i++) {
      Object field = partitionFieldGetters[i].getFieldOrNull(in);
      String partitionValue = field != null ? field.toString() : null;
      if (partitionValue == null || "".equals(partitionValue)) {
        partitionValue = defaultPartValue;
      }
      partSpec.put(partitionColumns[i], partitionValue);
    }
    return partSpec;
  }

  @Override
  public RowData projectColumnsToWrite(RowData in) {
    if (partitionIndexes.length == 0) {
      return in;
    }
    int len = nonPartitionIndexes.length;
    if (isCdc) {
      len += 1;
    }
    if (reuseRow == null) {
      this.reuseRow = new GenericRowData(len);
    }

    for (int i = 0; i < nonPartitionIndexes.length; i++) {
      reuseRow.setField(i, nonPartitionFieldGetters[i].getFieldOrNull(in));
    }
    if (isCdc) {
      reuseRow.setField(len - 1, FlinkUtil.rowKindToOperation(in.getRowKind()));
    }
    reuseRow.setRowKind(in.getRowKind());
    return reuseRow;
  }
}