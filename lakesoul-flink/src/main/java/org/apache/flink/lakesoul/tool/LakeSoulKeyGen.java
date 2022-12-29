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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.spark.sql.types.DataTypes.*;

public class LakeSoulKeyGen implements Serializable {

  public static final String DEFAULT_PARTITION_PATH = "default";
  private final GeneratedRecordComparator comparator;
  private boolean simpleRecordKey = false;
  private final int[] hashKeyIndex;
  private final LogicalType[] hashKeyType;

  public LakeSoulKeyGen(RowType rowType, String[] recordKeyFields) {
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    if (recordKeyFields.length == 1) {
      this.simpleRecordKey = true;
    }
    this.comparator = createSortComparator(getFieldPositions(recordKeyFields, fieldNames), rowType);
    this.hashKeyIndex = getFieldPositions(recordKeyFields, fieldNames);
    this.hashKeyType = Arrays.stream(hashKeyIndex).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
  }

  private static int[] getFieldPositions(String[] fields, List<String> allFields) {
    return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
  }

  private GeneratedRecordComparator createSortComparator(int[] sortIndices, RowType rowType) {
    SortSpec.SortSpecBuilder builder = SortSpec.builder();
    TableConfig tableConfig = new TableConfig();
    IntStream.range(0, sortIndices.length).forEach(i -> builder.addField(i, true, true));
    return new SortCodeGenerator(tableConfig, rowType, builder.build()).generateRecordComparator("comparator");
  }

  public long getRePartitionHash(RowData rowData) {
    long hash = 42;
    if (hashKeyType.length == 0) {
      return hash;
    }
    if (this.simpleRecordKey) {
      Object fieldOrNull = RowData.createFieldGetter(hashKeyType[0], hashKeyIndex[0]).getFieldOrNull(rowData);
      return getHash(hashKeyType[0], fieldOrNull, hash);
    } else {
      for (int i = 0; i < hashKeyType.length; i++) {
        Object fieldOrNull = RowData.createFieldGetter(hashKeyType[i], hashKeyIndex[i]).getFieldOrNull(rowData);
        hash = getHash(hashKeyType[i], fieldOrNull, hash);
      }
      return hash;
    }
  }

  public static long getHash(LogicalType type, Object field, long seed) {

    switch (type.getTypeRoot()) {
      case VARCHAR:
        UTF8String utf8String = UTF8String.fromString(java.lang.String.valueOf(field));
        seed = Murmur3HashFunction.hash(utf8String, StringType, seed);
        break;
      case INTEGER:
        seed = Murmur3HashFunction.hash(field, IntegerType, seed);
        break;
      case BIGINT:
        seed = Murmur3HashFunction.hash(field, LongType, seed);
        break;
      case BINARY:
        seed = Murmur3HashFunction.hash(field, ByteType, seed);
        break;
      case SMALLINT:
        seed = Murmur3HashFunction.hash(field, ShortType, seed);
        break;
      case FLOAT:
        seed = Murmur3HashFunction.hash(field, FloatType, seed);
        break;
      case DOUBLE:
        seed = Murmur3HashFunction.hash(field, DoubleType, seed);
        break;
      case BOOLEAN:
        seed = Murmur3HashFunction.hash(field, BooleanType, seed);
        break;
      default:
        throw new RuntimeException("not support this partition type now :" + type.getTypeRoot().toString());
    }
    return seed;
  }

  public GeneratedRecordComparator getComparator() {
    return comparator;
  }

}
