// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class LakeSoulKeyGen implements Serializable {

  private boolean simpleRecordKey = false;
  private final int[] hashKeyIndex;
  private final LogicalType[] hashKeyType;

  public LakeSoulKeyGen(RowType rowType, String[] recordKeyFields) {
    List<String> fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    if (recordKeyFields.length == 1) {
      this.simpleRecordKey = true;
    }
    this.hashKeyIndex = getFieldPositions(recordKeyFields, fieldNames);
    this.hashKeyType = Arrays.stream(hashKeyIndex).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
  }

  private static int[] getFieldPositions(String[] fields, List<String> allFields) {
    return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
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
      case DATE:
        seed = Murmur3HashFunction.hash(field,IntegerType, seed);
        break;
      case BOOLEAN:
        seed = Murmur3HashFunction.hash(field, BooleanType, seed);
        break;
      default:
        throw new RuntimeException("not support this partition type now :" + type.getTypeRoot().toString());
    }
    return seed;
  }
}
