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

package org.apache.flink.lakesoul.tool;

import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.meta.DataTypeUtil;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CDC_CHANGE_COLUMN;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.RECORD_KEY_NAME;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class FlinkUtil {
  private FlinkUtil() {
  }

  private static final String NOT_NULL = " NOT NULL";

  public static String convert(TableSchema schema) {
    return schema.toRowDataType().toString();
  }

  public static String getRangeValue(CatalogPartitionSpec cps) {
    return "Null";
  }

  public static StructType toSparkSchema(TableSchema tsc, Boolean isCdc) {
    StructType stNew = new StructType();

    for (int i = 0; i < tsc.getFieldCount(); i++) {
      String name = tsc.getFieldName(i).get();
      DataType dt = tsc.getFieldDataType(i).get();
      String dtName = dt.getLogicalType().getTypeRoot().name();
      stNew = stNew.add(name, DataTypeUtil.convertDatatype(dtName), dt.getLogicalType().isNullable());
    }
    if (isCdc) {
      stNew = stNew.add("rowKinds", StringType, true);
    }
    return stNew;
  }

  public static StringData rowKindToOperation(String rowKind) {
    if ("+I".equals(rowKind)) {
      return StringData.fromString("insert");
    }
    if ("-U".equals(rowKind)) {
      return StringData.fromString("delete");
    }
    if ("+U".equals(rowKind)) {
      return StringData.fromString("update");
    }
    if ("-D".equals(rowKind)) {
      return StringData.fromString("delete");
    }
    return null;
  }

  public static StringData rowKindToOperation(RowKind rowKind) {
    if (RowKind.INSERT.equals(rowKind)) {
      return StringData.fromString("insert");
    }
    if (RowKind.UPDATE_BEFORE.equals(rowKind)) {
      return StringData.fromString("delete");
    }
    if (RowKind.UPDATE_AFTER.equals(rowKind)) {
      return StringData.fromString("update");
    }
    if (RowKind.DELETE.equals(rowKind)) {
      return StringData.fromString("delete");
    }
    return null;
  }

  public static CatalogTable toFlinkCatalog(TableInfo tableInfo) {
    String tableSchema = tableInfo.getTableSchema();
    StructType struct = (StructType) org.apache.spark.sql.types.DataType.fromJson(tableSchema);
    Builder bd = Schema.newBuilder();
    JSONObject properties = tableInfo.getProperties();
    String lakesoulCdcColumnName = properties.getString(CDC_CHANGE_COLUMN);
    boolean contains = (lakesoulCdcColumnName != null && !"".equals(lakesoulCdcColumnName));
    String hashColumn = properties.getString(RECORD_KEY_NAME);
    for (StructField sf : struct.fields()) {
      if (contains && sf.name().equals(lakesoulCdcColumnName)) {
        continue;
      }
      String tyname = DataTypeUtil.convertToFlinkDatatype(sf.dataType().typeName());
      if (!sf.nullable()) {
        tyname += NOT_NULL;
      }
      bd = bd.column(sf.name(), tyname);
    }
    bd.primaryKey(Arrays.asList(hashColumn.split(",")));
    List<String> partitionData = Splitter.on(';').splitToList(tableInfo.getPartitions());
    List<String> parKey;
    if (partitionData.size()>1) {
      parKey = Splitter.on(',').splitToList(partitionData.get(0));
    } else {
      parKey = new ArrayList<>();
    }
    HashMap<String, String> conf = new HashMap<>();
    properties.forEach((key, value) -> conf.put(key, (String) value));
    return CatalogTable.of(bd.build(), "", parKey, conf);
  }

  public static String stringListToString(List<String> list) {
    if (list.isEmpty()) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    for (String s : list) {
      builder.append(s).append(",");
    }
    return builder.deleteCharAt(builder.length() - 1).toString();
  }

  public static String generatePartitionPath(LinkedHashMap<String, String> partitionSpec) {
    if (partitionSpec.isEmpty()) {
      return "";
    }
    StringBuilder suffixBuf = new StringBuilder();
    int i = 0;
    for (Map.Entry<String, String> e : partitionSpec.entrySet()) {
      if (i > 0) {
        suffixBuf.append("/");
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
      i++;
    }
    return suffixBuf.toString();
  }

  private static String escapePathName(String path) {
    if (path == null || path.length() == 0) {
      throw new TableException("Path should not be null or empty: " + path);
    }

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      sb.append(c);
    }
    return sb.toString();
  }

  public static List<String> getFieldNames(DataType dataType) {
    final LogicalType type = dataType.getLogicalType();
    if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
      return getFieldNames(dataType.getChildren().get(0));
    } else if (isCompositeType(type)) {
      return LogicalTypeChecks.getFieldNames(type);
    }
    return Collections.emptyList();
  }

  public static List<DataTypes.Field> getFields(DataType dataType, Boolean isCdc) {
    final List<String> names = getFieldNames(dataType);
    final List<DataType> dataTypes = getFieldDataTypes(dataType);
    if (isCdc) {
      names.add("rowKinds");
      dataTypes.add(DataTypes.VARCHAR(30));
    }
    return IntStream.range(0, names.size())
        .mapToObj(i -> DataTypes.FIELD(names.get(i), dataTypes.get(i)))
        .collect(Collectors.toList());
  }

  public static List<DataType> getFieldDataTypes(DataType dataType) {
    final LogicalType type = dataType.getLogicalType();
    if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE) {
      return getFieldDataTypes(dataType.getChildren().get(0));
    } else if (isCompositeType(type)) {
      return dataType.getChildren();
    }
    return Collections.emptyList();
  }

  public static Path makeQualifiedPath(String path) throws IOException {
    Path p = new Path(path);
    FileSystem fileSystem = p.getFileSystem();
    return p.makeQualified(fileSystem);
  }

  public static Path makeQualifiedPath(Path p) throws IOException {
    FileSystem fileSystem = p.getFileSystem();
    return p.makeQualified(fileSystem);
  }

  public static String getDatabaseName(String fullDatabaseName) {
    String[] splited = fullDatabaseName.split("\\.");
    return splited[splited.length - 1];
  }
}
