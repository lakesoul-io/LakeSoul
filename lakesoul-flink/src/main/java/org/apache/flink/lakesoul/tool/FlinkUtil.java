// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.DataFileInfo;
import com.dmetasoul.lakesoul.meta.DataOperation;
import com.dmetasoul.lakesoul.meta.LakeSoulOptions;
import com.dmetasoul.lakesoul.meta.MetaVersion;
import com.dmetasoul.lakesoul.meta.PartitionInfoScala;
import com.dmetasoul.lakesoul.meta.dao.TableInfoDao;
import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.SafetyNetWrapperFileSystem;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.ZoneId.SHORT_IDS;
import static org.apache.flink.lakesoul.tool.JobOptions.*;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;


public class FlinkUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkUtil.class);

    public static String convert(TableSchema schema) {
        return schema.toRowDataType().toString();
    }

    public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(RowType rowType, Optional<String> cdcColumn)
            throws CatalogException {
        List<Field> fields = new ArrayList<>();
        String cdcColName = null;
        if (cdcColumn.isPresent()) {
            cdcColName = cdcColumn.get();
            Field cdcField = ArrowUtils.toArrowField(cdcColName, new VarCharType(false, 16));
            fields.add(cdcField);
        }

        for (RowType.RowField field : rowType.getFields()) {
            String name = field.getName();
            if (name.equals(SORT_FIELD)) continue;

            LogicalType logicalType = field.getType();
            Field arrowField = ArrowUtils.toArrowField(name, logicalType);
            if (name.equals(cdcColName)) {
                if (!arrowField.toString().equals(fields.get(0).toString())) {
                    throw new CatalogException(CDC_CHANGE_COLUMN +
                            "=" +
                            cdcColName +
                            "has an invalid field of" +
                            field +
                            "," +
                            CDC_CHANGE_COLUMN +
                            " require field of " +
                            fields.get(0).toString());
                }
            } else {
                fields.add(arrowField);
            }
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
    }

    public static org.apache.arrow.vector.types.pojo.Schema toArrowSchema(TableSchema tableSchema,
                                                                          Optional<String> cdcColumn)
            throws CatalogException {
        List<Field> fields = new ArrayList<>();
        String cdcColName = null;
        if (cdcColumn.isPresent()) {
            cdcColName = cdcColumn.get();
            Field cdcField = ArrowUtils.toArrowField(cdcColName, new VarCharType(false, 16));
            fields.add(cdcField);
        }

        for (int i = 0; i < tableSchema.getFieldCount(); i++) {
            if (tableSchema.getTableColumn(i).get() instanceof TableColumn.ComputedColumn) {
                continue;
            }
            String name = tableSchema.getFieldName(i).get();
            DataType dataType = tableSchema.getFieldDataType(i).get();
            if (name.equals(SORT_FIELD)) continue;

            LogicalType logicalType = dataType.getLogicalType();
            Field arrowField = ArrowUtils.toArrowField(name, logicalType);
            if (name.equals(cdcColName)) {
                if (!arrowField.toString().equals(fields.get(0).toString())) {
                    throw new CatalogException(CDC_CHANGE_COLUMN +
                            "=" +
                            cdcColName +
                            "has an invalid field of" +
                            arrowField +
                            "," +
                            CDC_CHANGE_COLUMN +
                            " require field of " +
                            fields.get(0).toString());
                }
            } else {
                fields.add(arrowField);
            }
        }
        return new org.apache.arrow.vector.types.pojo.Schema(fields);
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

    private static final StringData INSERT = StringData.fromString("insert");
    private static final StringData UPDATE = StringData.fromString("update");
    private static final StringData DELETE = StringData.fromString("delete");

    public static RowKind operationToRowKind(StringData operation) {
        if (INSERT.equals(operation)) {
            return RowKind.INSERT;
        }
        if (UPDATE.equals(operation)) {
            return RowKind.UPDATE_AFTER;
        }
        if (DELETE.equals(operation)) {
            return RowKind.DELETE;
        }
        return null;
    }

    public static boolean isCDCDelete(StringData operation) {
        return StringData.fromString("delete").equals(operation);
    }

    public static boolean isView(TableInfo tableInfo) {
        JSONObject jsb = DBUtil.stringToJSON(tableInfo.getProperties());
        if (jsb.containsKey(LAKESOUL_VIEW.key()) &&
                "true".equals(jsb.getString(LAKESOUL_VIEW.key())) &&
                LAKESOUL_VIEW_KIND.equals(jsb.getString(LAKESOUL_VIEW_TYPE.key()))) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isTable(TableInfo tableInfo) {
        JSONObject jsb = DBUtil.stringToJSON(tableInfo.getProperties());
        if (jsb.containsKey(LAKESOUL_VIEW.key()) && "true".equals(jsb.getString(LAKESOUL_VIEW.key()))) {
            return false;
        } else {
            return true;
        }
    }

    public static CatalogBaseTable toFlinkCatalog(TableInfo tableInfo) {
        String tableSchema = tableInfo.getTableSchema();
        JSONObject properties = JSON.parseObject(tableInfo.getProperties());
        properties.put(CATALOG_PATH.key(), tableInfo.getTablePath());

        org.apache.arrow.vector.types.pojo.Schema arrowSchema = null;
        if (TableInfoDao.isArrowKindSchema(tableSchema)) {
            try {
                arrowSchema = org.apache.arrow.vector.types.pojo.Schema.fromJSON(tableSchema);
            } catch (IOException e) {
                throw new CatalogException(e);
            }
        } else {
            StructType struct = (StructType) org.apache.spark.sql.types.DataType.fromJson(tableSchema);
            arrowSchema = org.apache.spark.sql.arrow.ArrowUtils.toArrowSchema(struct, ZoneId.of("UTC").toString());
        }
        RowType rowType = ArrowUtils.fromArrowSchema(arrowSchema);
        Builder bd = Schema.newBuilder();

        String lakesoulCdcColumnName = properties.getString(CDC_CHANGE_COLUMN);
        boolean contains = (lakesoulCdcColumnName != null && !lakesoulCdcColumnName.isEmpty());

        for (RowType.RowField field : rowType.getFields()) {
            if (contains && field.getName().equals(lakesoulCdcColumnName)) {
                continue;
            }
            bd.column(field.getName(), field.getType().asSerializableString());
        }
        if (properties.getString(COMPUTE_COLUMN_JSON) != null) {
            JSONObject computeColumnJson = JSONObject.parseObject(properties.getString(COMPUTE_COLUMN_JSON));
            computeColumnJson.forEach((column, columnExpr) -> bd.columnByExpression(column, (String) columnExpr));
        }

        DBUtil.TablePartitionKeys partitionKeys = DBUtil.parseTableInfoPartitions(tableInfo.getPartitions());
        if (!partitionKeys.primaryKeys.isEmpty()) {
            bd.primaryKey(partitionKeys.primaryKeys);
        }

        if (properties.getString(WATERMARK_SPEC_JSON) != null) {
            JSONObject watermarkJson = JSONObject.parseObject(properties.getString(WATERMARK_SPEC_JSON));
            watermarkJson.forEach((column, watermarkExpr) -> bd.watermark(column, (String) watermarkExpr));
        }


        List<String> parKeys = partitionKeys.rangeKeys;
        HashMap<String, String> conf = new HashMap<>();
        properties.forEach((key, value) -> conf.put(key, value.toString()));
        if (FlinkUtil.isView(tableInfo)) {
            return CatalogView.of(bd.build(), "", properties.getString(VIEW_ORIGINAL_QUERY),
                    properties.getString(VIEW_EXPANDED_QUERY), conf);
        } else {
            return CatalogTable.of(bd.build(), "", parKeys, conf);

        }
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
            names.add(CDC_CHANGE_COLUMN_DEFAULT);
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

    public static void setIOConfigs(Configuration conf, NativeIOBase io) {
        conf.addAll(GlobalConfiguration.loadConfiguration());
        try {
            FlinkUtil.class.getClassLoader().loadClass("org.apache.hadoop.hdfs.HdfsConfiguration");
            org.apache.hadoop.conf.Configuration
                    hadoopConf =
                    HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
            String defaultFS = hadoopConf.get("fs.defaultFS");
            io.setObjectStoreOption("fs.defaultFS", defaultFS);
        } catch (Exception e) {
            // ignore
        }
        if (conf.containsKey(DEFAULT_FS.key())) {
            setFSConf(conf, DEFAULT_FS.key(), DEFAULT_FS.key(), io);
        }

        // try hadoop's s3 configs
        setFSConf(conf, "fs.s3a.access.key", "fs.s3a.access.key", io);
        setFSConf(conf, "fs.s3a.secret.key", "fs.s3a.secret.key", io);
        setFSConf(conf, "fs.s3a.endpoint", "fs.s3a.endpoint", io);
        setFSConf(conf, "fs.s3a.endpoint.region", "fs.s3a.endpoint.region", io);
        setFSConf(conf, "fs.s3a.path.style.access", "fs.s3a.path.style.access", io);
        // try flink's s3 credential configs
        setFSConf(conf, S3_ACCESS_KEY.key(), "fs.s3a.access.key", io);
        setFSConf(conf, S3_SECRET_KEY.key(), "fs.s3a.secret.key", io);
        setFSConf(conf, S3_ENDPOINT.key(), "fs.s3a.endpoint", io);
        setFSConf(conf, "s3.endpoint.region", "fs.s3a.endpoint.region", io);
        setFSConf(conf, S3_PATH_STYLE_ACCESS.key(), "fs.s3a.path.style.access", io);
        setFSConf(conf, S3_BUCKET.key(), "fs.s3a.bucket", io);

        // try other native options
        for (ConfigOption<String> option : NativeOptions.OPTION_LIST) {
            String value = conf.get(option);
            if (value != null) {
                int lastDot = option.key().lastIndexOf('.');
                String key = lastDot == -1 ? option.key() : option.key().substring(lastDot + 1);
                io.setOption(key, value);
            }
        }
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
        if (typeRoot == LogicalTypeRoot.VARCHAR)
            return StringData.fromString(valStr);
        if ("null".equals(valStr)) return null;

        switch (typeRoot) {
            case CHAR:
            case BOOLEAN:
                return Boolean.parseBoolean(valStr);
            case TINYINT:
                return Byte.parseByte(valStr);
            case SMALLINT:
                return Short.parseShort(valStr);
            case INTEGER:
            case DATE:
                return Integer.parseInt(valStr);
            case BIGINT:
                return Long.parseLong(valStr);
            case FLOAT:
                return Float.parseFloat(valStr);
            case DOUBLE:
                return Double.parseDouble(valStr);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampData.fromLocalDateTime(LocalDateTime.parse(valStr));
            default:
                throw new RuntimeException(
                        String.format(
                                "Can not convert %s to type %s for partition value", valStr, type));
        }
    }

    public static DataFileInfo[] getTargetDataFileInfo(TableInfo tif, List<Map<String, String>> remainingPartitions) {
        if (remainingPartitions == null) {
            return DataOperation.getTableDataInfo(tif.getTableId());
        } else {
            List<String> partitionDescs = remainingPartitions.stream()
                    .map(DBUtil::formatPartitionDesc)
                    .collect(Collectors.toList());
            List<PartitionInfoScala> partitionInfos = new ArrayList<>();
            for (String partitionDesc : partitionDescs) {
                partitionInfos.add(MetaVersion.getSinglePartitionInfo(tif.getTableId(), partitionDesc, ""));
            }
            PartitionInfoScala[] ptinfos = partitionInfos.toArray(new PartitionInfoScala[0]);
            return DataOperation.getTableDataInfo(ptinfos);
        }
    }

    public static Map<String, Map<Integer, List<Path>>> splitDataInfosToRangeAndHashPartition(
            TableInfo tableInfo,
            DataFileInfo[] dataFileInfoArray) {
        Map<String, Map<Integer, List<Path>>> splitByRangeAndHashPartition = new LinkedHashMap<>();
        for (DataFileInfo dataFileInfo : dataFileInfoArray) {
            if (isExistHashPartition(tableInfo) && dataFileInfo.file_bucket_id() != -1) {
                splitByRangeAndHashPartition.computeIfAbsent(
                                dataFileInfo.range_partitions(),
                                k -> new LinkedHashMap<>())
                        .computeIfAbsent(dataFileInfo.file_bucket_id(), v -> new ArrayList<>())
                        .add(new Path(dataFileInfo.path()));
            } else {
                splitByRangeAndHashPartition.computeIfAbsent(
                                dataFileInfo.range_partitions(),
                                k -> new LinkedHashMap<>())
                        .computeIfAbsent(-1, v -> new ArrayList<>())
                        .add(new Path(dataFileInfo.path()));
            }
        }
        return splitByRangeAndHashPartition;
    }

    public static DataFileInfo[] getSinglePartitionDataFileInfo(TableInfo tif, String partitionDesc) {
        PartitionInfoScala partitionInfo = MetaVersion.getSinglePartitionInfo(tif.getTableId(), partitionDesc, "");
        return DataOperation.getTableDataInfo(new PartitionInfoScala[]{partitionInfo});
    }

    public static int[] getFieldPositions(String[] fields, List<String> allFields) {
        return Arrays.stream(fields).mapToInt(allFields::indexOf).toArray();
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
        JSONObject tableProperties = JSON.parseObject(tif.getProperties());
        if (tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM()) &&
                tableProperties.getString(LakeSoulOptions.HASH_BUCKET_NUM()).equals("-1")) {
            return false;
        } else {
            return tableProperties.containsKey(LakeSoulOptions.HASH_BUCKET_NUM());
        }
    }

    public static ZoneId getLocalTimeZone(Configuration configuration) {
        return ZoneId.of("UTC");
//        String zone = configuration.getString(TableConfigOptions.LOCAL_TIME_ZONE);
//        validateTimeZone(zone);
//        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
//                ? ZoneId.systemDefault()
//                : ZoneId.of(zone);
    }

    /**
     * Validates user configured time zone.
     */
    private static void validateTimeZone(String zone) {
        final String zoneId = zone.toUpperCase();
        if (zoneId.startsWith("UTC+")
                || zoneId.startsWith("UTC-")
                || SHORT_IDS.containsKey(zoneId)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                                    + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is '%s'.",
                            zone));
        }
    }

    public static void setLocalTimeZone(Configuration options, ZoneId localTimeZone) {
        options.setString(TableConfigOptions.LOCAL_TIME_ZONE, localTimeZone.toString());
    }

    public static void setS3Options(Configuration dstConf, Configuration srcConf) {
        if (srcConf.contains(S3_ACCESS_KEY)) {
            dstConf.set(S3_ACCESS_KEY, srcConf.get(S3_ACCESS_KEY));
        }
        if (srcConf.contains(S3_SECRET_KEY)) {
            dstConf.set(S3_SECRET_KEY, srcConf.get(S3_SECRET_KEY));
        }
        if (srcConf.contains(S3_ENDPOINT)) {
            dstConf.set(S3_ENDPOINT, srcConf.get(S3_ENDPOINT));
        }
        if (srcConf.contains(S3_BUCKET)) {
            dstConf.set(S3_BUCKET, srcConf.get(S3_BUCKET));
        }
        if (srcConf.contains(S3_PATH_STYLE_ACCESS)) {
            dstConf.set(S3_PATH_STYLE_ACCESS, srcConf.get(S3_PATH_STYLE_ACCESS));
        }
        if (srcConf.contains(DEFAULT_FS)) {
            dstConf.set(DEFAULT_FS, srcConf.get(DEFAULT_FS));
        }
    }

    public static JSONObject getPropertiesFromConfiguration(Configuration conf) {
        Map<String, Object> map = new HashMap<>();
        map.put(USE_CDC.key(), String.valueOf(conf.getBoolean(USE_CDC)));
        map.put(HASH_BUCKET_NUM.key(), String.valueOf(conf.getInteger(BUCKET_PARALLELISM)));
        map.put(CDC_CHANGE_COLUMN, conf.getString(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT));
        return new JSONObject(map);
    }

    public static boolean hasHdfsClasses() {
        try {
            FlinkUtil.class.getClassLoader().loadClass("org.apache.flink.runtime.fs.hdfs.HadoopFileSystem");
            FlinkUtil.class.getClassLoader().loadClass("org.apache.hadoop.hdfs.DistributedFileSystem");
            return true;
        } catch (ClassNotFoundException e) {
            LOG.info("HDFS classes not in classpath, ignore dir permission setting");
            return false;
        }
    }

    public static boolean hasS3Classes() {
        try {
            FlinkUtil.class.getClassLoader().loadClass("org.apache.flink.fs.s3.common.FlinkS3FileSystem");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    public static void createAndSetTableDirPermission(Path p, boolean ignoreTableDirExists) throws IOException {
        // TODO: move these to native io
        // currently we only support setting owner and permission for HDFS.
        // S3 support will be added later
        if (!hasHdfsClasses()) return;

        FileSystem fs = p.getFileSystem();
        if ((fs instanceof HadoopFileSystem)
                || (fs instanceof SafetyNetWrapperFileSystem
                && ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate() instanceof HadoopFileSystem)) {
            String userName = DBUtil.getUser();
            String domain = DBUtil.getDomain();
            HadoopFileSystem hfs = fs instanceof HadoopFileSystem ? (HadoopFileSystem) fs
                    : (HadoopFileSystem) ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();
            org.apache.hadoop.fs.FileSystem hdfs = hfs.getHadoopFileSystem();

            LOG.info("Set dir {} permission for {}:{} with flink fs {}, hadoop fs {}", p, userName, domain,
                    hfs.getClass(), hdfs.getClass());

            if (userName == null || domain == null)
                return;

            org.apache.hadoop.fs.Path nsDir = HadoopFileSystem.toHadoopPath(p.getParent());
            if (!hdfs.exists(nsDir)) {
                hdfs.mkdirs(nsDir);
                if (domain.equalsIgnoreCase("public") || domain.equalsIgnoreCase("lake-public")) {
                    hdfs.setPermission(nsDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
                } else {
                    hdfs.setOwner(nsDir, userName, domain);
                    hdfs.setPermission(nsDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
                }
            }
            org.apache.hadoop.fs.Path tbDir = HadoopFileSystem.toHadoopPath(p);
            if (!hdfs.exists(tbDir)) {
                hdfs.mkdirs(tbDir);
            } else {
                if (ignoreTableDirExists) {
                    return;
                } else {
                    throw new IOException("Table dir " + tbDir + " already exists");
                }
            }
            if (domain.equalsIgnoreCase("public") || domain.equalsIgnoreCase("lake-public")) {
                hdfs.setPermission(tbDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
            } else {
                hdfs.setOwner(tbDir, userName, domain);
                hdfs.setPermission(tbDir, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE));
            }
        }
    }

    public static String serializeWatermarkSpec(List<WatermarkSpec> watermarkSpecs) {
        Map<String, String> map = new HashMap<>();
        for (WatermarkSpec watermarkSpec : watermarkSpecs) {
            // Deserialize: watermarkSpec.getWatermarkExprOutputType() will be inferred from LakeSoul TableSchema
            map.put(watermarkSpec.getRowtimeAttribute(), watermarkSpec.getWatermarkExpr());
        }
        return JSON.toJSONString(map);
    }


}
