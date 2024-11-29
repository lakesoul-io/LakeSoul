package com.dmetasoul.lakesoul.lakesoul.local;

import com.dmetasoul.lakesoul.lakesoul.LakeSoulArrowUtils;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import com.dmetasoul.lakesoul.lakesoul.local.arrow.ArrowBatchWriter;
import com.dmetasoul.lakesoul.meta.DBConfig;
import com.dmetasoul.lakesoul.meta.DBManager;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.*;

import static com.dmetasoul.lakesoul.meta.DBConfig.TableInfoProperty;

/**
 * LakeSoulLocalJavaWriter is responsible for writing LakeSoul data in a local environment.
 * <p>
 * This class implements the AutoCloseable interface, supporting automatic resource management.
 * <p>
 * Key functionalities include:
 * 1. Initializing and configuring the LakeSoul writer
 * 2. Managing Arrow batch data
 * 3. Handling table information and metadata
 * 4. Setting up file system and S3-related configurations
 * 5. Supporting writing of various data types
 * <p>
 * When using this class, it's important to correctly set database connection parameters,
 * table information, and file system configurations.
 */
public class LakeSoulLocalJavaWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulLocalJavaWriter.class);

    // Key for table name
    public static final String TABLE_NAME = "table_name";
    // Key for PostgreSQL URL
    public static final String PG_URL_KEY = DBUtil.urlKey;
    // Key for username
    public static final String USERNAME_KEY = DBUtil.usernameKey;
    // Key for password
    public static final String PWD_KEY = DBUtil.passwordKey;

    // Key for default file system
    public static final String DEFAULT_FS = "fs.defaultFS";

    // Key for S3 access key
    public static final String S3_ACCESS_KEY = "s3.access-key";
    // Key for S3 secret key
    public static final String S3_SECRET_KEY = "s3.secret-key";
    // Key for S3 endpoint
    public static final String S3_ENDPOINT = "s3.endpoint";
    // Key for S3 path style access
    public static final String S3_PATH_STYLE_ACCESS = "s3.path.style.access";
    // Key for S3 bucket
    public static final String S3_BUCKET = "s3.bucket";

    // Key for lakesoul cdc column
    public static final String CDC_COLUMN_KEY = "lakesoul.cdc.column";
    // Default value for lakesoul cdc column
    public static final String CDC_COLUMN_DEFAULT_VALUE = "rowKinds";

    // Key for memory limit of native writer
    public static final String MEM_LIMIT = "lakesoul.native_writer.mem_limit";

    // Key for keeping orders of native writer
    public static final String KEEP_ORDERS = "lakesoul.native_writer.keep_orders";

    public static final List<String> NATIVE_OPTION_LIST = Arrays.asList(MEM_LIMIT, KEEP_ORDERS);

    private NativeIOWriter nativeWriter;
    private VectorSchemaRoot batch;
    private ArrowBatchWriter arrowWriter;
    private int rowsInBatch;
    private long totalRows = 0;
    private TableInfo tableInfo;
    private DBManager dbManager;
    private Map<String, String> params;

    String cdcColumn = null;

    public static void setIOConfigs(Map<String, String> conf, NativeIOBase io) {

        if (conf.containsKey(DEFAULT_FS)) {
            setFSConf(conf, DEFAULT_FS, DEFAULT_FS, io);
        }

        // try hadoop's s3 configs
        setFSConf(conf, "fs.s3a.access.key", "fs.s3a.access.key", io);
        setFSConf(conf, "fs.s3a.secret.key", "fs.s3a.secret.key", io);
        setFSConf(conf, "fs.s3a.endpoint", "fs.s3a.endpoint", io);
        setFSConf(conf, "fs.s3a.endpoint.region", "fs.s3a.endpoint.region", io);
        setFSConf(conf, "fs.s3a.path.style.access", "fs.s3a.path.style.access", io);
        // try flink's s3 credential configs
        setFSConf(conf, S3_ACCESS_KEY, "fs.s3a.access.key", io);
        setFSConf(conf, S3_SECRET_KEY, "fs.s3a.secret.key", io);
        setFSConf(conf, S3_ENDPOINT, "fs.s3a.endpoint", io);
        setFSConf(conf, "s3.endpoint.region", "fs.s3a.endpoint.region", io);
        setFSConf(conf, S3_PATH_STYLE_ACCESS, "fs.s3a.path.style.access", io);
        setFSConf(conf, S3_BUCKET, "fs.s3a.bucket", io);

        // try other native options
        for (String option : NATIVE_OPTION_LIST) {
            String value = conf.get(option);
            if (value != null) {
                int lastDot = option.lastIndexOf('.');
                String key = lastDot == -1 ? option : option.substring(lastDot + 1);
                io.setOption(key, value);
            }
        }
    }

    public static void setFSConf(Map<String, String> conf, String confKey, String fsConfKey, NativeIOBase io) {
        String value = conf.getOrDefault(confKey, "");
        if (!value.isEmpty()) {
            LOG.info("Set native object store option {}={}", fsConfKey, value);
            io.setObjectStoreOption(fsConfKey, value);
        }
    }


    public void init(Map<String, String> params) throws IOException {
        LOG.info(String.format("LakeSoulLocalJavaWriter init with params=%s", params));

        this.params = params;
        Preconditions.checkArgument(params.containsKey(PG_URL_KEY));
        Preconditions.checkArgument(params.containsKey(USERNAME_KEY));
        Preconditions.checkArgument(params.containsKey(PWD_KEY));
        System.setProperty(DBUtil.urlKey, params.get(PG_URL_KEY));
        System.setProperty(DBUtil.usernameKey, params.get(USERNAME_KEY));
        System.setProperty(DBUtil.passwordKey, params.get(PWD_KEY));
        dbManager = new DBManager();

        tableInfo = dbManager.getTableInfoByName(params.get(TABLE_NAME));

        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, String> properties = mapper.readValue(tableInfo.getProperties(), Map.class);
            cdcColumn = properties.get(DBConfig.TableInfoProperty.CDC_CHANGE_COLUMN);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        initNativeWriter();

    }

    private void initNativeWriter() throws IOException {
        LOG.info(String.format("LakeSoulLocalJavaWriter initNativeWriter with tableInfo=%s", tableInfo));
        nativeWriter = new NativeIOWriter(tableInfo);

        Schema arrowSchema = LakeSoulArrowUtils.cdcColumnAlignment(Schema.fromJSON(tableInfo.getTableSchema()), cdcColumn);

        batch = VectorSchemaRoot.create(arrowSchema, nativeWriter.getAllocator());
        arrowWriter = ArrowBatchWriter.createWriter(batch);

        setIOConfigs(params, nativeWriter);
        nativeWriter.initializeWriter();
    }

    void write(Object[] row) {
        this.arrowWriter.write(row);
        this.rowsInBatch++;
        this.totalRows++;
    }

    public void writeAddRow(Object[] row) {
//        System.out.println(Arrays.toString(row));
        if (cdcColumn != null) {
            Object[] addRow = new Object[row.length + 1];
            for (int i = 0; i < row.length; i++) {
                addRow[i] = row[i];
            }
            addRow[row.length] = "insert";
            write(addRow);
        } else {
            write(row);
        }
    }

    public void writeDeleteRow(Object[] row) {
        Preconditions.checkArgument(cdcColumn != null, "DeleteRow is not support for Non Cdc Table");
        Object[] delRow = new Object[row.length + 1];
        for (int i = 0; i < row.length; i++) {
            delRow[i] = row[i];
        }
        delRow[row.length] = "delete";
        write(delRow);
    }


    public void commit() throws IOException {
        LOG.info(String.format("LakeSoulLocalJavaWriter commit batch size = %s, batch schema=%s", batch.getRowCount(), batch.getSchema().toJson()));
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);

        List<DataCommitInfo> commitInfoList = new ArrayList<>();
        HashMap<String, List<NativeIOWriter.FlushResult>> partitionDescAndFilesMap = this.nativeWriter.flush();
        for (Map.Entry<String, List<NativeIOWriter.FlushResult>> entry : partitionDescAndFilesMap.entrySet()) {
            commitInfoList.add(createDataCommitInfo(entry.getKey(), entry.getValue()));
        }

        LOG.info(String.format("Committing DataCommitInfo=%s", commitInfoList));
        for (DataCommitInfo commitInfo : commitInfoList) {
            dbManager.commitDataCommitInfo(commitInfo, Collections.emptyList());
        }

        recreateWriter();
        this.batch.clear();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
    }

    private void recreateWriter() throws IOException {
        try {
            close();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        initNativeWriter();
    }

    private DataCommitInfo createDataCommitInfo(String partitionDesc, List<NativeIOWriter.FlushResult> fileList) {
        DataCommitInfo.Builder builder = DataCommitInfo.newBuilder()
                .setTableId(tableInfo.getTableId())
                .setPartitionDesc(partitionDesc)
                .setCommitId(DBUtil.toProtoUuid(UUID.randomUUID()))
                .setCommitted(false)
                .setCommitOp(CommitOp.AppendCommit)
                .setTimestamp(System.currentTimeMillis())
                .setDomain("public");
        for (NativeIOWriter.FlushResult file : fileList) {
            DataFileOp.Builder fileOp = DataFileOp.newBuilder()
                    .setFileOp(FileOp.add)
                    .setPath(file.getFilePath())
                    .setSize(file.getFileSize())
                    .setFileExistCols(file.getFileExistCols());
            builder.addFileOps(fileOp.build());
        }

        return builder.build();
    }


    @Override
    public void close() throws Exception {
        if (batch != null) {
            batch.close();
            batch = null;
        }
        if (nativeWriter != null) {
            nativeWriter.close();
            nativeWriter = null;
        }
    }

    public static class ArrowTypeMockDataGenerator
            implements ArrowType.ArrowTypeVisitor<Object> {

        long count = 0;

        static long mod = 511;

        public static final ArrowTypeMockDataGenerator INSTANCE = new ArrowTypeMockDataGenerator();

        @Override
        public Object visit(ArrowType.Null aNull) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Struct struct) {
            return null;
        }

        @Override
        public Object visit(ArrowType.List list) {
            return null;
        }

        @Override
        public Object visit(ArrowType.LargeList largeList) {
            return null;
        }

        @Override
        public Object visit(ArrowType.FixedSizeList fixedSizeList) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Union union) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Map map) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Int type) {
            int bitWidth = type.getBitWidth();
            long value = (count++) % mod;
            if (bitWidth <= 8) return (byte) value;
            if (bitWidth <= 2 * 8) return (short) value;
            if (bitWidth <= 4 * 8) return (int) value;
            return value;
        }

        @Override
        public Object visit(ArrowType.FloatingPoint type) {
            double value = ((double) (count++)) / mod;
            switch (type.getPrecision()) {
                case HALF:
                case SINGLE:
                    return (float) value;
            }
            return value;
        }

        @Override
        public Object visit(ArrowType.Utf8 utf8) {
            return String.valueOf((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.LargeUtf8 largeUtf8) {
            return String.valueOf((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.Binary binary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.LargeBinary largeBinary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.FixedSizeBinary fixedSizeBinary) {
            return String.valueOf((count++) % mod).getBytes();
        }

        @Override
        public Object visit(ArrowType.Bool bool) {
            return (count++) % 2 == 0;
        }

        @Override
        public Object visit(ArrowType.Decimal decimal) {
            return new BigDecimal(((double) (count++)) / mod).setScale(decimal.getScale(), BigDecimal.ROUND_UP);
        }

        @Override
        public Object visit(ArrowType.Date date) {
            return (int) ((count++) % mod);
        }

        @Override
        public Object visit(ArrowType.Time time) {
            long value = (count++) % mod;
            return new Timestamp(value * 1_000_000_000);
        }

        @Override
        public Object visit(ArrowType.Timestamp timestamp) {
            long value = (count++) % mod;
            return new java.sql.Timestamp(value * 1000); // 将秒转换为毫秒
        }

        @Override
        public Object visit(ArrowType.Interval interval) {
            return null;
        }

        @Override
        public Object visit(ArrowType.Duration duration) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {

        DBManager meta = new DBManager();

        boolean cdc = true;
        meta.cleanMeta();
        String tableId = "table_" + UUID.randomUUID();
        List<String> primaryKeys = Arrays.asList("id");
        List<String> partitionKeys = Arrays.asList("range");
//        List<String> partitionKeys = Collections.emptyList();
        String partition = DBUtil.formatTableInfoPartitionsField(
                primaryKeys,
                partitionKeys
        );

        List<Field> fields;
        if (cdc) {
            fields = Arrays.asList(
                    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("utf8", FieldType.nullable(new ArrowType.Utf8()), null)
                    , new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null)
                    , new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null)
                    , new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)
                    , new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null)
                    , new Field(TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT, FieldType.notNullable(new ArrowType.Utf8()), null)
                    , new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null)
            );
        } else {
            fields = Arrays.asList(
                    new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("range", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
                    , new Field("utf8", FieldType.nullable(new ArrowType.Utf8()), null)
                    , new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null)
                    , new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null)
                    , new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null)
                    , new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null)
                    , new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null)
            );
        }
        Schema schema = new Schema(fields);

        ObjectMapper objectMapper = new ObjectMapper();

        Map<String, String> properties = new HashMap<>();
        if (!primaryKeys.isEmpty()) {
            properties.put(TableInfoProperty.HASH_BUCKET_NUM, "4");
            properties.put("hashPartitions",
                    String.join(DBConfig.LAKESOUL_HASH_PARTITION_SPLITTER, primaryKeys));
            if (cdc) {
                properties.put(TableInfoProperty.USE_CDC, "true");
                properties.put(TableInfoProperty.CDC_CHANGE_COLUMN,
                        TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT);
            }
        }

        meta.createTable(
                tableId,
                "default",
                "test_local_java_table",
                "file:/tmp/test_local_java_table",
                schema.toJson(),
                objectMapper.writeValueAsString(properties),
                partition);

        LakeSoulLocalJavaWriter localJavaWriter = new LakeSoulLocalJavaWriter();

        Map<String, String> params = new HashMap<>();
        params.put("lakesoul.pg.url", "jdbc:postgresql://127.0.0.1:5433/test_lakesoul_meta?stringtype=unspecified");
        params.put("lakesoul.pg.username", "yugabyte");
        params.put("lakesoul.pg.password", "yugabyte");
        params.put(TABLE_NAME, "test_local_java_table");

        localJavaWriter.init(params);
        int times = 8;
        int ranges = 13;
        for (int t = 0; t < times; t++) {
            int numRows = 1024;
            int numCols = fields.size();
            for (int i = 0; i < numRows; i++) {
                Object[] row = new Object[cdc ? numCols - 1 : numCols];
                for (int j = 0, k = 0; j < numCols; j++) {
                    if (fields.get(j).getName().contains(TableInfoProperty.CDC_CHANGE_COLUMN_DEFAULT)) {
                        continue;
                    } else if (fields.get(j).getName().contains("id")) {
                        row[k++] = i;
                    } else if (fields.get(j).getName().contains("range")) {
                        row[k++] = i % ranges;
                    } else {
                        row[k++] = fields.get(j).getType().accept(ArrowTypeMockDataGenerator.INSTANCE);
                    }
                }
                localJavaWriter.writeAddRow(row);
                if (cdc && i % 7 == 0) {
                    localJavaWriter.writeDeleteRow(row);
                }
            }
            localJavaWriter.commit();
        }


        assert meta.getAllPartitionInfo(tableId).size() == times;

        System.out.println("data commit DONE");
        localJavaWriter.close();
    }
}
