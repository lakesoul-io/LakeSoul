// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter.FlushResult;
import com.dmetasoul.lakesoul.meta.BucketingUtils;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.MetaUtils;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.datasources.LakeSoulFileWriter;
import org.apache.spark.sql.internal.SQLConf$;
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf;
import org.apache.spark.sql.lakesoul.utils.TableInfo;
import org.apache.spark.sql.vectorized.NativeIOOptions;
import org.apache.spark.sql.vectorized.NativeIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC;

public class CompactBucketIO implements AutoCloseable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CompactBucketIO.class);

    public static String DISCARD_FILE_LIST_KEY = "discard_file";
    public static String COMPACT_DIR = "compact_dir";
    public static String INCREMENTAL_FILE = "incremental_file";
    private final List<String> primaryKeys;
    private final List<String> rangeColumns;
    private final int maxRowGroupRows;
    private final boolean tableHashBucketNumChanged;
    private NativeIOReader nativeIOReader;
    private final Schema schema;
    private Schema partitionSchema;
    private final int hashBucketNum;
    private final NativeIOOptions nativeIOOptions;
    private final List<CompressDataFileInfo> fileInfo;
    private final String metaPartitionExpr;
    private NativeIOWriter nativeWriter;
    private LakeSoulArrowReader lakesoulArrowReader;
    private Map<String, List<CompressDataFileInfo>> levelFileMap;
    private final String tablePath;
    private final FileSystem fileSystem;
    private final int compactionExistedFileNumberLimit;
    private final long compactionMergeFileSizeLimit;
    private final int compactionMergeFileNumLimit;
    private final long compactionReadFileMaxSize;
    private final int readFileNumLimit;
    private final long batchIncrementalFileSizeLimit;
    private final int batchSize;
    private final long taskId;

    public CompactBucketIO(Configuration conf, List<CompressDataFileInfo> fileInfo, TableInfo tableInfo,
                           String tablePath, String metaPartitionExpr,
                           int tableHashBucketNum, int readFileNumLimit, long batchIncrementalFileSizeLimit,
                           boolean tableHashBucketNumChanged, long taskId)
            throws IOException {

        this.fileInfo = fileInfo;
        this.metaPartitionExpr = metaPartitionExpr;
        this.schema = Schema.fromJSON(tableInfo.table_schema());
        this.primaryKeys = JavaConverters.seqAsJavaList(tableInfo.hash_partition_columns().toSeq());
        this.hashBucketNum = tableHashBucketNum;
        if (StringUtils.isNotBlank(tableInfo.range_column())) {
            this.rangeColumns = Arrays.stream(tableInfo.range_column().split(",")).collect(Collectors.toList());
        } else {
            this.rangeColumns = Collections.emptyList();
        }
        if (!this.rangeColumns.isEmpty()) {
            List<Field> partitionFields = rangeColumns.stream().map(schema::findField).collect(Collectors.toList());
            this.partitionSchema = new Schema(partitionFields);
        }
        this.nativeIOOptions = NativeIOUtils.getNativeIOOptions(conf, new Path(this.fileInfo.get(0).getFilePath()));
        this.fileSystem = FileSystem.get(conf);

        this.maxRowGroupRows = conf.getInt(LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE().key(),
                (int) LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE().defaultValue().get());
        this.batchSize = conf.getInt(SQLConf$.MODULE$.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(), 256);
        this.tablePath = tablePath;

        this.compactionExistedFileNumberLimit = conf.getInt(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_NUM_LIMIT().key(),
                (int) LakeSoulSQLConf.COMPACTION_LEVEL_FILE_NUM_LIMIT().defaultValue().get());
        this.compactionMergeFileSizeLimit =
                DBUtil.parseMemoryExpression(conf.get(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_SIZE_LIMIT().key(),
                        LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE().defaultValue().get()));
        this.compactionMergeFileNumLimit = conf.getInt(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT().key(),
                (int) LakeSoulSQLConf.COMPACTION_LEVEL_FILE_MERGE_NUM_LIMIT().defaultValue().get());
        this.compactionReadFileMaxSize =
                DBUtil.parseMemoryExpression(conf.get(LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE().key(),
                        LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE().defaultValue().get()));

        this.readFileNumLimit = readFileNumLimit;
        this.batchIncrementalFileSizeLimit = Math.min(batchIncrementalFileSizeLimit, compactionReadFileMaxSize);
        this.tableHashBucketNumChanged = tableHashBucketNumChanged;
        this.taskId = taskId;

        this.initLevelFile(this.fileInfo);
    }

    private void initializeReader(List<CompressDataFileInfo> filePath) throws IOException {
        nativeIOReader = new NativeIOReader();
        for (CompressDataFileInfo path : filePath) {
            nativeIOReader.addFile(path.getFilePath());
        }
        nativeIOReader.setSchema(this.schema);
        if (this.primaryKeys != null) {
            nativeIOReader.setPrimaryKeys(this.primaryKeys);
        }
        scala.collection.immutable.Map<String, String> partitionMapFromKey = MetaUtils.getPartitionMapFromKey(
                metaPartitionExpr);
        for (Map.Entry<String, String> entry : JavaConverters.mapAsJavaMapConverter(partitionMapFromKey).asJava()
                .entrySet()) {
            nativeIOReader.setDefaultColumnValue(entry.getKey(), entry.getValue());
        }
        if (this.partitionSchema != null) {
            nativeIOReader.setPartitionSchema(this.partitionSchema);
        }
        NativeIOUtils.setNativeIOOptions(this.nativeIOReader, this.nativeIOOptions);
        nativeIOReader.setBatchSize(this.batchSize);

        nativeIOReader.initializeReader();
        lakesoulArrowReader = new LakeSoulArrowReader(this.nativeIOReader, 10000);
        LOG.info("Task {}, Initialized compaction reader for table {}, pk {}, files {}, partitions {}, batch {}",
                taskId, tablePath, primaryKeys, filePath, partitionSchema, batchSize);
    }

    private void initializeWriter(String outPath) throws IOException {
        nativeWriter = new NativeIOWriter(this.schema);
        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);
        nativeWriter.setPrimaryKeys(this.primaryKeys);

        nativeWriter.setHashBucketNum(this.hashBucketNum);
        if (this.tableHashBucketNumChanged) {
            nativeWriter.setRangePartitions(rangeColumns);
            nativeWriter.useDynamicPartition(true);
            nativeWriter.withPrefix(outPath);
        } else {
            if (!this.metaPartitionExpr.equals(LAKESOUL_NON_PARTITION_TABLE_PART_DESC)) {
                nativeWriter.withPrefix(String.format("%s/%s", outPath, metaPartitionExpr.replace(",", "/")));
            } else {
                nativeWriter.withPrefix(outPath);
            }
            Option<Object> hashBucketId = BucketingUtils.getBucketId(this.fileInfo.get(0).getFilePath());
            if (hashBucketId.isEmpty()) {
                nativeWriter.setOption(LakeSoulFileWriter.HASH_BUCKET_ID_KEY(), "0");
            } else {
                nativeWriter.setOption(LakeSoulFileWriter.HASH_BUCKET_ID_KEY(),
                        String.valueOf(hashBucketId.get()));
            }
        }

        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);

        NativeIOUtils.setNativeIOOptions(nativeWriter, this.nativeIOOptions);
        nativeWriter.initializeWriter();
        LOG.info("Task {}, Initialized compaction writer for table {}, outPath {}, pks {}, range {}",
                taskId, tablePath, outPath, primaryKeys, rangeColumns);
    }

    private HashMap<String, List<FlushResult>> readAndWrite() throws Exception {
        VectorSchemaRoot currentVCR = null;
        try {
            while (this.lakesoulArrowReader.hasNext()) {
                currentVCR = this.lakesoulArrowReader.nextResultVectorSchemaRoot();
                nativeWriter.write(currentVCR);
                currentVCR.close();
            }
            return this.nativeWriter.flush();
        } finally {
            if (currentVCR != null) {
                currentVCR.close();
            }
        }
    }

    private void initLevelFile(List<CompressDataFileInfo> fileList) {
        if (this.levelFileMap == null) {
            this.levelFileMap = new HashMap<>();
        }
        for (CompressDataFileInfo fileInfo : fileList) {
            if (fileInfo.getFilePath().contains(COMPACT_DIR)) {
                this.levelFileMap.computeIfAbsent(COMPACT_DIR, COMPACT_FILE -> new ArrayList<>()).add(fileInfo);
            } else {
                this.levelFileMap.computeIfAbsent(INCREMENTAL_FILE, INCREMENTAL_FILE -> new ArrayList<>())
                        .add(fileInfo);
            }
        }
        LOG.info("Task {}, Initialized level file for table {}, files {}, levelFileMap {}",
                taskId, tablePath, fileList, levelFileMap);
    }

    public HashMap<String, List<CompressDataFileInfo>> startCompactTask() throws Exception {
        List<CompressDataFileInfo> resultList = new ArrayList<>();
        HashMap<String, List<CompressDataFileInfo>> rsMap = new HashMap<>();
        LOG.info("Task {}, Starting compact task, fileInfo {}", taskId, this.fileInfo);
        if (this.tableHashBucketNumChanged) {
            List<CompressDataFileInfo> fileList = this.fileInfo;
            int index = 0;
            while (index < fileList.size()) {
                long batchFileSize = 0;
                List<CompressDataFileInfo> batchFileList = new ArrayList<>();
                while (index < fileList.size() && batchFileList.size() < readFileNumLimit) {
                    CompressDataFileInfo curFile = fileList.get(index);
                    batchFileList.add(curFile);
                    batchFileSize += curFile.getFileSize();
                    index++;
                    if (batchFileSize > compactionReadFileMaxSize) {
                        break;
                    }
                }
                initializeReader(batchFileList);
                initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                HashMap<String, List<FlushResult>> outFile = readAndWrite();
                this.close();
                if (outFile == null || outFile.isEmpty()) {
                    LOG.info("change tableHashBucketNum task: compaction task read file list is {}", batchFileList);
                    LOG.info("change tableHashBucketNum task: after compaction out file info: {}", outFile);
                    throw new IllegalStateException(
                            "change tableHashBucketNum task: after compaction, without out file info, read file list is: " +
                                    batchFileList);
                }
                for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                    resultList.addAll(changeFlushFileToCompressDataFileInfo(entry.getValue()));
                }
            }
            rsMap.put(this.metaPartitionExpr, resultList);
            if (levelFileMap.containsKey(COMPACT_DIR)) {
                rsMap.put(DISCARD_FILE_LIST_KEY, levelFileMap.get(COMPACT_DIR));
            }
        } else {
            if (levelFileMap.containsKey(INCREMENTAL_FILE)) {
                List<CompressDataFileInfo> totalIncreFileList = levelFileMap.get(INCREMENTAL_FILE);
                int index = 0;
                while (index < totalIncreFileList.size()) {
                    long batchFileSize = 0;
                    List<CompressDataFileInfo> batchFileList = new ArrayList<>();
                    while (index < totalIncreFileList.size() && batchFileList.size() < readFileNumLimit) {
                        CompressDataFileInfo curFile = totalIncreFileList.get(index);
                        batchFileList.add(curFile);
                        batchFileSize += curFile.getFileSize();
                        index++;
                        if (batchFileSize > batchIncrementalFileSizeLimit) {
                            break;
                        }
                    }
                    LOG.info("Task {}, Compacting incremental file", taskId);
                    initializeReader(batchFileList);
                    initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                    HashMap<String, List<FlushResult>> outFile = readAndWrite();
                    this.close();
                    if (outFile == null || outFile.isEmpty()) {
                        LOG.info("incremental level compaction task: read file list is {}", batchFileList);
                        LOG.info("incremental level compaction task: after compaction out file info: {}", outFile);
                        throw new IllegalStateException(
                                "incremental level compaction task: after compaction, without out file info, read file list is: " +
                                        batchFileList);
                    }
                    for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                        levelFileMap.computeIfAbsent(COMPACT_DIR, COMPACT_DIR -> new ArrayList<>())
                                .addAll(changeFlushFileToCompressDataFileInfo(entry.getValue()));
                    }
                }
            }
            List<CompressDataFileInfo> oriCompactFileList = levelFileMap.get(COMPACT_DIR);
            LOG.info("Task {}, Start to compact existed compact file {}, num {}",
                    taskId, oriCompactFileList, oriCompactFileList.size());
            List<CompressDataFileInfo> discardFileList = new ArrayList<>();
            if (oriCompactFileList.size() >= compactionExistedFileNumberLimit) {
                int index = 0;
                long fileSize = 0L;
                List<CompressDataFileInfo> curMergeList = new ArrayList<>();
                while (index < oriCompactFileList.size()) {
                    CompressDataFileInfo curFile = oriCompactFileList.get(index);
                    if (curFile.getFileSize() < compactionReadFileMaxSize ||
                            !curMergeList.isEmpty()) {
                        fileSize += curFile.getFileSize();
                        curMergeList.add(curFile);
                        discardFileList.add(curFile);
                    } else {
                        resultList.add(curFile);
                    }

                    if (curMergeList.size() >= this.compactionMergeFileNumLimit ||
                        fileSize >= this.compactionMergeFileSizeLimit) {
                        LOG.info("Task {}, Compacting existed compact files, curMergeList {}, size {}",
                                taskId, curMergeList, fileSize);
                        initializeReader(curMergeList);
                        initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                        HashMap<String, List<FlushResult>> outFile = readAndWrite();
                        this.close();
                        if (outFile == null || outFile.isEmpty()) {
                            LOG.info("COMPACT_DIR level compaction task: read file list is {}", curMergeList);
                            LOG.info("COMPACT_DIR level compaction task: after compaction out file info: {}", outFile);
                            throw new IllegalStateException(
                                    "COMPACT_DIR level compaction task: after compaction, without out file info, read file list is: " +
                                            curMergeList);
                        }
                        for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                            resultList.addAll(changeFlushFileToCompressDataFileInfo(entry.getValue()));
                        }
                        curMergeList.clear();
                        fileSize = 0L;
                    }
                    index++;
                }
                if (!curMergeList.isEmpty()) {
                    resultList.addAll(curMergeList);
                    discardFileList.removeAll(curMergeList);
                }
                rsMap.put(this.metaPartitionExpr, resultList);
                rsMap.put(DISCARD_FILE_LIST_KEY, discardFileList);
            } else {
                rsMap.put(this.metaPartitionExpr, oriCompactFileList);
            }
        }
        return rsMap;
    }

    private List<CompressDataFileInfo> changeFlushFileToCompressDataFileInfo(List<FlushResult> flushResultList) {
        List<CompressDataFileInfo> compressDataFileInfoList = new ArrayList<>();
        flushResultList.forEach(file -> {
            String filePath = file.getFilePath();
            Path path = new Path(file.getFilePath());
            String fileExistCols = file.getFileExistCols();
            if (fileExistCols.startsWith("arrow_schema,")) {
                fileExistCols = fileExistCols.replace("arrow_schema,", "");
            }
            try {
                FileStatus fileStatus = fileSystem.getFileStatus(path);
                compressDataFileInfoList.add(new CompressDataFileInfo(filePath, fileStatus.getLen(), fileExistCols,
                        fileStatus.getModificationTime()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        LOG.info("Task {}, Finished compact file list {}", taskId, compressDataFileInfoList);
        return compressDataFileInfoList;
    }

    @Override
    public void close() throws Exception {
        if (this.nativeIOReader != null) {
            this.nativeIOReader.close();
            this.nativeIOReader = null;
        }
        if (this.nativeWriter != null) {
            this.nativeWriter.close();
            this.nativeWriter = null;
        }
    }
}
