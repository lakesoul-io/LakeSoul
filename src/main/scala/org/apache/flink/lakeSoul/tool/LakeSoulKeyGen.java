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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class LakeSoulKeyGen implements Serializable {

  private static final String NULL_RECORD_KEY_PLACEHOLDER = "__null__";
  private static final String EMPTY_RECORD_KEY_PLACEHOLDER = "__empty__";
  public static final String DEFAULT_PARTITION_PATH = "default";
  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = ";";
  private final Configuration conf;
  private final String[] recordKeyFields;
  private final String[] partitionPathFields;
  private final RowDataProjection recordKeyProjection;
  private final RowDataProjection partitionPathProjection;
  private RowData.FieldGetter recordKeyFieldGetter;
  private RowData.FieldGetter partitionPathFieldGetter;
  private boolean nonPartitioned;
  private boolean simpleRecordKey = false;
  private boolean simplePartitionPath = false;
  private final List<String> fieldNames;
  private List<String> partitionKey;
  private String simpleRecordKeyType;


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
  }

  public static String objToString(@Nullable Object obj) {
    return obj == null ? null : obj.toString();
  }

  public static int objToInt(@Nullable Object obj) {
    return obj == null ? null : (int) obj;
  }

  public static Long objToLong(@Nullable Object obj) {
    return obj == null ? null : (long) obj;
  }

  public String[] getRecordKeyFields() {
    String keyField = conf.getString(LakeSoulSinkOptions.KEY_FIELD);
    return keyField.split(",");
  }

  private String[] getPartitionFiled() {
    String partitionField = conf.getString(LakeSoulSinkOptions.PARTITION_FIELD);
    return partitionField.split(",");
  }

  public String getRecordKey(RowData rowData) throws Exception {
    if (this.simpleRecordKey) {
      return getRecordKey(recordKeyFieldGetter.getFieldOrNull(rowData), this.recordKeyFields[0]);
    } else {
      Object[] keyValues = this.recordKeyProjection.projectAsValues(rowData);
      return getRecordKey(keyValues, this.recordKeyFields);
    }
  }

  public int getSimpleIntKey(RowData rowData) throws Exception {
    return objToInt(recordKeyFieldGetter.getFieldOrNull(rowData));
  }

  public Long getSimpleLongKey(RowData rowData) throws Exception {
    return objToLong(recordKeyFieldGetter.getFieldOrNull(rowData));
  }

  private static RowDataProjection getProjection(String[] fields, List<String> schemaFields, List<LogicalType> schemaTypes) {
    int[] positions = getFieldPositions(fields, schemaFields);
    LogicalType[] types = Arrays.stream(positions).mapToObj(schemaTypes::get).toArray(LogicalType[]::new);
    return RowDataProjection.instance(types, positions);
  }

  private static int[] getFieldPositions(String[] fields, List<String> allFields) {
    return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
  }

  public static String getRecordKey(Object recordKeyValue, String recordKeyField) throws FileNotFoundException {
    String recordKey = objToString(recordKeyValue);
    if (recordKey == null || recordKey.isEmpty()) {
      throw new FileNotFoundException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
    }
    return recordKey;
  }

  private static String getRecordKey(Object[] keyValues, String[] keyFields) throws FileNotFoundException {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (int i = 0; i < keyValues.length; i++) {
      String recordKeyField = keyFields[i];
      String recordKeyValue = objToString(keyValues[i]);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField).append(":").append(NULL_RECORD_KEY_PLACEHOLDER).append(",");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField).append(":").append(EMPTY_RECORD_KEY_PLACEHOLDER).append(",");
      } else {
        recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(",");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new FileNotFoundException("recordKey values: \"" + recordKey + "\" for fields: "
          + Arrays.toString(keyFields) + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }

  public static String getPartitionPath(
      Object partValue) {
    String partitionPath = objToString(partValue);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_PARTITION_PATH;
    }
    return partitionPath;
  }

  public String getPartitionPath(RowData rowData) {
    if (this.simplePartitionPath) {
      return getPartitionPath(partitionPathFieldGetter.getFieldOrNull(rowData)
      );
    } else if (this.nonPartitioned) {
      return DEFAULT_PARTITION_PATH;
    } else {
      Object[] partValues = this.partitionPathProjection.projectAsValues(rowData);
      return getRecordPartitionPath(partValues, this.partitionPathFields);
    }
  }

  private static String getRecordPartitionPath(
      Object[] partValues,
      String[] partFields) {
    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partFields.length; i++) {
      String partValue = objToString(partValues[i]);
      if (partValue == null || partValue.isEmpty()) {
        partitionPath.append(DEFAULT_PARTITION_PATH);
      } else {
        partitionPath.append(partValue);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  public String getRePartitionKey(RowData row) throws Exception {
    return getPartitionPath(row);
  }

  public String getRecordKeyType() throws NoSuchMethodException {
    if (!"".equals(simpleRecordKeyType) && simpleRecordKeyType != null) {
      return simpleRecordKeyType;
    }
    throw new NoSuchMethodException("Multiple primary keys are not supported now");
  }

  public List<String> getPartitionKey() {
    return partitionKey;
  }

  public void setPartitionKey(List<String> partitionKey) {
    this.partitionKey = partitionKey;
  }
}
