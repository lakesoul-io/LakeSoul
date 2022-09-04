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

package org.apache.flink.lakeSoul.tool;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.spark.sql.catalyst.expressions.Murmur3HashFunction;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class LakeSoulKeyGen implements Serializable {

  public static final String DEFAULT_PARTITION_PATH = "default";
  private final Configuration conf;
  private final String[] recordKeyFields;
  private final String[] partitionPathFields;
  private final RowDataProjection recordKeyProjection;
  private final RowDataProjection partitionPathProjection;
  private RowData.FieldGetter recordKeyFieldGetter;
  private RowData.FieldGetter partitionPathFieldGetter;
  private final GeneratedRecordComparator comparator;
  private RecordComparator compareFunction;
  private boolean nonPartitioned;
  private boolean simpleRecordKey = false;
  private boolean simplePartitionPath = false;
  private final List<String> fieldNames;
  private List<String> partitionKey;
  private String simpleRecordKeyType;
//  private int[] partitionKeyIndex;
//  private LogicalType[] partitionKeyType;
  private int[] hashKeyIndex;
  private LogicalType[] hashKeyType;

  public LakeSoulKeyGen(RowType rowType, Configuration conf, List<String> partitionKey) {
    this.conf = conf;
    this.recordKeyFields = getRecordKeyFields();
    this.partitionKey = partitionKey;
    this.partitionPathFields = getPartitionFiled();
    this.fieldNames = rowType.getFieldNames();
    List<LogicalType> fieldTypes = rowType.getChildren();
    if (this.recordKeyFields.length == 1) {
      this.simpleRecordKey = true;
      int recordKeyIdx = fieldNames.indexOf(this.recordKeyFields[0]);
      LogicalType logicalType = fieldTypes.get(recordKeyIdx);
      simpleRecordKeyType = logicalType.toString();
      this.recordKeyFieldGetter = RowData.createFieldGetter(logicalType, recordKeyIdx);
      this.recordKeyProjection = null;
    } else {
      this.recordKeyProjection = getProjection(this.recordKeyFields, fieldNames, fieldTypes);
    }
    this.comparator = createSortComparator(getFieldPositions(this.recordKeyFields, fieldNames), rowType);
    if (this.partitionPathFields.length == 1) {
      this.simplePartitionPath = true;
      if (this.partitionPathFields[0].equals("")) {
        this.nonPartitioned = true;
      } else {
        int partitionPathIdx = fieldNames.indexOf(this.partitionPathFields[0]);
        this.partitionPathFieldGetter = RowData.createFieldGetter(fieldTypes.get(partitionPathIdx), partitionPathIdx);
      }
      this.partitionPathProjection = null;
    } else {
      this.partitionPathProjection = getProjection(this.partitionPathFields, fieldNames, fieldTypes);
    }
//    this.partitionKeyIndex = getFieldPositions(this.partitionPathFields, fieldNames);
//    this.partitionKeyType = Arrays.stream(partitionKeyIndex).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
    this.hashKeyIndex = getFieldPositions(this.recordKeyFields, fieldNames);
    this.hashKeyType = Arrays.stream(hashKeyIndex).mapToObj(fieldTypes::get).toArray(LogicalType[]::new);
  }

  public static String objToString(@Nullable Object obj) {
    return obj == null ? null : obj.toString();
  }

  public String[] getRecordKeyFields() {
    String keyField = conf.getString(LakeSoulSinkOptions.KEY_FIELD);
    return keyField.split(",");
  }

  private String[] getPartitionFiled() {
    String partitionField = conf.getString(LakeSoulSinkOptions.PARTITION_FIELD);
    return partitionField.split(",");
  }

  private static RowDataProjection getProjection(String[] fields, List<String> schemaFields, List<LogicalType> schemaTypes) {
    int[] positions = getFieldPositions(fields, schemaFields);
    LogicalType[] types = Arrays.stream(positions).mapToObj(schemaTypes::get).toArray(LogicalType[]::new);
    return RowDataProjection.instance(types, positions);
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

  public String getRePartitionHash(RowData rowData) throws Exception {
    int hash = 42;
    if(hashKeyType.length==0){
      return hash+"";
    }
    if (this.simpleRecordKey) {
      Object fieldOrNull = RowData.createFieldGetter(hashKeyType[0], hashKeyIndex[0]).getFieldOrNull(rowData);
      return getHash(hashKeyType[0], fieldOrNull, hash)+"";
    } else {
      for (int i = 0; i < hashKeyType.length; i++) {
        Object fieldOrNull = RowData.createFieldGetter(hashKeyType[i], hashKeyIndex[i]).getFieldOrNull(rowData);
        hash = getHash(hashKeyType[i], fieldOrNull, hash);
      }
      return hash+"";
    }
  }

  public int getHash(LogicalType type, Object filed, int seed) {

    switch (type.getTypeRoot()) {
      case VARCHAR:
        UTF8String utf8String = UTF8String.fromString(java.lang.String.valueOf(filed));
        seed = (int) Murmur3HashFunction.hash(utf8String, StringType, seed);
        break;
      case INTEGER:
        seed =(int) Murmur3HashFunction.hash((int) filed,IntegerType, seed);
        break;
      case BIGINT:
        seed = (int) Murmur3HashFunction.hash((long) filed,LongType, seed);
        break;
      case BINARY:
        seed = (int) Murmur3HashFunction.hash((byte) filed,ByteType, seed);
        break;
      case SMALLINT:
        seed = (int) Murmur3HashFunction.hash((short) filed,ShortType, seed);
        break;
      case FLOAT:
        seed = (int) Murmur3HashFunction.hash((float) filed, FloatType,seed);
        break;
      case DOUBLE:
        seed = (int) Murmur3HashFunction.hash((double) filed, DoubleType,seed);
        break;
      case BOOLEAN:
        seed = (int) Murmur3HashFunction.hash((boolean) filed ,BooleanType, seed);
        break;
      default:
        throw new RuntimeException("not support this partition type now :" + type.getTypeRoot().toString());
    }
    return seed;
  }

  public GeneratedRecordComparator getComparator() {
    return comparator;
  }

  public RecordComparator getCompareFunction() {
    return compareFunction;
  }

  public void setCompareFunction(RecordComparator compareFunction) {
    this.compareFunction = compareFunction;
  }
}
