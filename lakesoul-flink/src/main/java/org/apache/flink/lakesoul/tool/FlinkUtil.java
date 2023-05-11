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
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import com.dmetasoul.lakesoul.meta.*;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.FieldVector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.guava30.com.google.common.base.Splitter;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.CDC_CHANGE_COLUMN;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.RECORD_KEY_NAME;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;
import static org.apache.spark.sql.types.DataTypes.StringType;


public class FlinkUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtil.class);

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

    public static RowKind operationToRowKind(StringData operation) {
        if (StringData.fromString("insert").equals(operation)) {
            return RowKind.INSERT;
        }
        if (StringData.fromString("update").equals(operation)) {
            return RowKind.UPDATE_AFTER;
        }
        if (StringData.fromString("delete").equals(operation)) {
            return RowKind.DELETE;
        }
        return null;
    }
    public static boolean isCDCDelete(StringData operation){
        if (StringData.fromString("delete").equals(operation)) {
            return true;
        }else{
            return false;
        }
    }

    public static CatalogTable toFlinkCatalog(TableInfo tableInfo) {
        String tableSchema = tableInfo.getTableSchema();
        StructType struct = (StructType) org.apache.spark.sql.types.DataType.fromJson(tableSchema);
        Builder bd = Schema.newBuilder();
        JSONObject properties = tableInfo.getProperties();
        String lakesoulCdcColumnName = properties.getString(CDC_CHANGE_COLUMN);
        boolean contains = (lakesoulCdcColumnName != null && !"".equals(lakesoulCdcColumnName));
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
        List<String> partitionData = Splitter.on(';').splitToList(tableInfo.getPartitions());
        List<String> parKeys;
        String parKey = partitionData.get(0);
        String hashKey = partitionData.get(1);
        if (!"".equals(hashKey)) {
            List<String> hashKeys = Splitter.on(',').splitToList(hashKey);
            bd.primaryKey(hashKeys);
        }
        if (!"".equals(parKey)) {
            parKeys = Splitter.on(',').splitToList(parKey);
        } else {
            parKeys = new ArrayList<>();
        }
        HashMap<String, String> conf = new HashMap<>();
        properties.forEach((key, value) -> conf.put(key, (String) value));
        return CatalogTable.of(bd.build(), "", parKeys, conf);
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

    public static void setFSConfigs(Configuration conf, NativeIOBase io) {
        conf.addAll(GlobalConfiguration.loadConfiguration());
        // try hadoop's s3 configs
        setFSConf(conf, "fs.s3a.access.key", "fs.s3a.access.key", io);
        setFSConf(conf, "fs.s3a.secret.key", "fs.s3a.secret.key", io);
        setFSConf(conf, "fs.s3a.endpoint", "fs.s3a.endpoint", io);
        setFSConf(conf, "fs.s3a.endpoint.region", "fs.s3a.endpoint.region", io);
        // try flink's s3 credential configs
        setFSConf(conf, "s3.access-key", "fs.s3a.access.key", io);
        setFSConf(conf, "s3.secret-key", "fs.s3a.secret.key", io);
        setFSConf(conf, "s3.endpoint", "fs.s3a.endpoint", io);
    }

    public static void setFSConf(Configuration conf, String confKey, String fsConfKey, NativeIOBase io) {
        String value = conf.getString(confKey, "");
        if (!value.isEmpty()) {
            LOG.info("Set native object store option {}={}", fsConfKey, value);
            io.setObjectStoreOption(fsConfKey, value);
        }
    }


    public static Object convertStringToInternalValue(String valStr, LogicalType type) {
        if (valStr == null) {
            return null;
        }
        LogicalTypeRoot typeRoot = type.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString(valStr);
            case BOOLEAN:
                return Boolean.parseBoolean(valStr);
            case TINYINT:
                return Byte.parseByte(valStr);
            case SMALLINT:
                return Short.parseShort(valStr);
            case INTEGER:
                return Integer.parseInt(valStr);
            case BIGINT:
                return Long.parseLong(valStr);
            case FLOAT:
                return Float.parseFloat(valStr);
            case DOUBLE:
                return Double.parseDouble(valStr);
            case DATE:
                return (int) LocalDate.parse(valStr).toEpochDay();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(LocalDateTime.parse(valStr));
            default:
                throw new RuntimeException(
                        String.format(
                                "Can not convert %s to type %s for partition value", valStr, type));
        }
    }

    public static DataFileInfo[] getTargetDataFileInfo(TableInfo tif, List<Map<String, String>> remainingPartitions) {
        if (remainingPartitions == null || remainingPartitions.size() == 0) {
            return DataOperation.getTableDataInfo(tif.getTableId());
        } else {
            List<String> partitionDescs = remainingPartitions.stream()
                    .map(map -> map.entrySet().stream()
                            .map(entry -> entry.getKey() + "=" + entry.getValue())
                            .collect(Collectors.joining(",")))
                    .collect(Collectors.toList());
            List<PartitionInfo> partitionInfos = new ArrayList<>();
            for (String partitionDesc : partitionDescs) {
                partitionInfos.add(MetaVersion.getSinglePartitionInfo(tif.getTableId(), partitionDesc, ""));
            }
            PartitionInfo[] ptinfos = partitionInfos.toArray(new PartitionInfo[partitionInfos.size()]);
            return DataOperation.getTableDataInfo(ptinfos);
        }
    }

    public static Map<String, Map<Integer, List<Path>>> splitDataInfosToRangeAndHashPartition(String tid, DataFileInfo[] dfinfos) {
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        TableInfo tif = DataOperation.dbManager().getTableInfoByTableId(tid);
        for (DataFileInfo pif : dfinfos) {
            if (isExistHashPartition(tif) && pif.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(pif.file_bucket_id(), v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(pif.range_partitions(), k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(new Path(pif.path()));
            }
        }
        return splitByRangeAndHashPartition;
    }

    public static DataFileInfo[] getSinglePartitionDataFileInfo(TableInfo tif, String partitionDesc) {
        PartitionInfo partitionInfo = MetaVersion.getSinglePartitionInfo(tif.getTableId(), partitionDesc, "");
        return DataOperation.getTableDataInfo(new PartitionInfo[]{partitionInfo});
    }

    public static TableEnvironment createTableEnvInBatchMode(SqlDialect dialect) {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tableEnv.getConfig().setSqlDialect(dialect);
        return tableEnv;
    }

    public static <R> R getType(Schema.UnresolvedColumn unresolvedColumn) {
        // TODO: 2023/4/19 hard-code for pass suite
        throw new RuntimeException("org.apache.flink.lakesoul.tool.FlinkUtil.getType");
    }

    public static boolean isExistHashPartition(TableInfo tif) {
        JSONObject tableProperties = tif.getProperties();
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM()) && tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
        }
    }
}
