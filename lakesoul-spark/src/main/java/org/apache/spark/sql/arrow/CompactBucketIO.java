// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter.FlushResult;
import com.dmetasoul.lakesoul.meta.DBUtil;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf;
import org.apache.spark.sql.lakesoul.utils.TableInfo;
import org.apache.spark.sql.vectorized.NativeIOOptions;
import org.apache.spark.sql.vectorized.NativeIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CompactBucketIO implements AutoCloseable, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(CompactBucketIO.class);

    public static String  DISCARD_FILE_LIST_KEY = "discard_file";
    private String COMPACT_DIR = "compact_dir";
    private String INCREMENTAL_FILE = "incremental_file";

    // private TableInfo tableInfo;
    private NativeIOReader nativeIOReader;

    private Schema schema;
    private final List<String> primaryKeys;
    private final List<String> rangeColumns;
    private Schema partitionSchema;
    private int hashBucketNum;

    private NativeIOOptions nativeIOOptions;

    List<FlushResult> fileInfo;
    LinkedHashMap<String, String> partition;

    private NativeIOWriter nativeWriter;
    private final int maxRowGroupRows;

    private LakeSoulArrowReader lakesoulArrowReader;
    private VectorSchemaRoot currentVCR;

    private Map<String, List<FlushResult>> levelFileMap;
    private String tablePath;

    private int compactionReadFileNumberLimit;
    private long compactionReadFileSize;
    private int readFileNumLimit;
    private long batchIncrementalFileSizeLimit;
    private final boolean tableHashBucketNumChanged;

    public CompactBucketIO(Configuration conf, List<FlushResult> fileInfo, TableInfo tableInfo, String tablePath, LinkedHashMap<String, String> partition,
                           int tableHashBucketNum, int readFileNumLimit, long batchIncrementalFileSizeLimit, boolean tableHashBucketNumChanged)
            throws IOException {

        this.fileInfo = fileInfo;
        this.partition = partition;
        this.schema = Schema.fromJSON(tableInfo.table_schema());
        this.primaryKeys = JavaConverters.seqAsJavaList(tableInfo.hash_partition_columns().toSeq());
        this.hashBucketNum = tableHashBucketNum;
        if (StringUtils.isNotBlank(tableInfo.range_column())) {
            this.rangeColumns = Arrays.stream(tableInfo.range_column().split(",")).collect(Collectors.toList());
        } else {
            this.rangeColumns = Arrays.asList();
        }
        if (this.rangeColumns.size() > 0) {
            List<Field> partitionFields = rangeColumns.stream().map(schema::findField).collect(Collectors.toList());
            this.partitionSchema = new Schema(partitionFields);
        }
        this.nativeIOOptions = NativeIOUtils.getNativeIOOptions(conf, new Path(this.fileInfo.get(0).getFilePath()));

        this.maxRowGroupRows = conf.getInt(LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE().key(),
                (int) LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE().defaultValue().get());
        this.tablePath = tablePath;

        this.compactionReadFileNumberLimit = conf.getInt(LakeSoulSQLConf.COMPACTION_LEVEL_FILE_NUM_LIMIT().key(),
                (int) LakeSoulSQLConf.COMPACTION_LEVEL_FILE_NUM_LIMIT().defaultValue().get());
        this.compactionReadFileSize = DBUtil.parseMemoryExpression(conf.get(LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE().key(),
                LakeSoulSQLConf.COMPACTION_LEVEL_MAX_FILE_SIZE().defaultValue().get()));


        this.readFileNumLimit = readFileNumLimit;
        this.batchIncrementalFileSizeLimit = Math.min(batchIncrementalFileSizeLimit, compactionReadFileSize);
        this.tableHashBucketNumChanged = tableHashBucketNumChanged;

        this.initLevelFile(this.fileInfo);
    }

    private void initializeReader(List<FlushResult> filePath) throws IOException {
        nativeIOReader = new NativeIOReader();
        for (FlushResult path : filePath) {
            nativeIOReader.addFile(path.getFilePath());
        }
        nativeIOReader.setSchema(this.schema);
        if (this.primaryKeys != null) {
            nativeIOReader.setPrimaryKeys(this.primaryKeys);
        }
        for (Map.Entry<String, String> entry : this.partition.entrySet()) {
            nativeIOReader.setDefaultColumnValue(entry.getKey(), entry.getValue());
        }
        // nativeIOReader.setRangePartitions(rangeColumns);
        if (this.partitionSchema != null) {
            nativeIOReader.setPartitionSchema(this.partitionSchema);
        }
        NativeIOUtils.setNativeIOOptions(this.nativeIOReader, this.nativeIOOptions);

        // nativeIOReader.setBatchSize(capacity);
        nativeIOReader.initializeReader();
        lakesoulArrowReader = new LakeSoulArrowReader(this.nativeIOReader, 10000);
    }

    private void initializeWriter(String outPath) throws IOException {
        nativeWriter = new NativeIOWriter(this.schema);
        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);
        nativeWriter.setPrimaryKeys(this.primaryKeys);
        nativeWriter.setRangePartitions(this.rangeColumns);
        nativeWriter.withPrefix(outPath);
        nativeWriter.useDynamicPartition(true);
        nativeWriter.setHashBucketNum(this.hashBucketNum);

        nativeWriter.setRowGroupRowNumber(this.maxRowGroupRows);

        NativeIOUtils.setNativeIOOptions(nativeWriter, this.nativeIOOptions);
        nativeWriter.initializeWriter();
    }

    private HashMap<String, List<FlushResult>> readAndWrite() throws Exception {
        while (this.lakesoulArrowReader.hasNext()) {
            this.currentVCR = this.lakesoulArrowReader.nextResultVectorSchemaRoot();
            nativeWriter.write(this.currentVCR);
            this.currentVCR.close();
        }
        return this.nativeWriter.flush();
    }

    private void initLevelFile(List<FlushResult> fileList) {
        if (this.levelFileMap == null) {
            this.levelFileMap = new HashMap<>();
        }
        for (FlushResult fileInfo : fileList) {
            if (fileInfo.getFilePath().contains(COMPACT_DIR)) {
                this.levelFileMap.computeIfAbsent(COMPACT_DIR, COMPACT_FILE -> new ArrayList<>()).add(fileInfo);
            } else {
                this.levelFileMap.computeIfAbsent(INCREMENTAL_FILE, INCREMENTAL_FILE -> new ArrayList<>()).add(fileInfo);
            }
        }
    }

    public HashMap<String, List<FlushResult>> startCompactTask() throws Exception {
        List<FlushResult> resultList = new ArrayList<>();
        HashMap<String, List<FlushResult>> rsMap = new HashMap<>();
        String partitionKey = null;
        if (this.tableHashBucketNumChanged) {
            List<FlushResult> fileList = this.fileInfo;
            int index = 0;
            while (index < fileList.size()) {
                int batchFileNum = 0;
                long batchFileSize = 0;
                List<FlushResult> batchFileList = new ArrayList<>();
                while (index < fileList.size() && batchFileNum < readFileNumLimit) {
                    FlushResult curFile = fileList.get(index);
                    batchFileList.add(curFile);
                    batchFileNum++;
                    batchFileSize += curFile.getFileSize();
                    index++;
                    if (batchFileSize > compactionReadFileSize) {
                        break;
                    }
                }
                initializeReader(batchFileList);
                initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                HashMap<String, List<FlushResult>> outFile = readAndWrite();
                this.close();
                if (outFile == null || outFile.size() < 1) {
                    LOG.info("change tableHashBucketNum task: compaction task read file list is {}", batchFileList);
                    LOG.info("change tableHashBucketNum task: after compaction out file info: " + outFile);
                    throw new IllegalStateException("change tableHashBucketNum task: after compaction, without out file info, read file list is: " + batchFileList);
                }
                for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                    partitionKey = entry.getKey();
                    resultList.addAll(entry.getValue());
                }
            }
            rsMap.put(partitionKey, resultList);
            if (levelFileMap.containsKey(COMPACT_DIR)) {
                rsMap.put(DISCARD_FILE_LIST_KEY, levelFileMap.get(COMPACT_DIR));
            }
        } else {
            if (levelFileMap.containsKey(INCREMENTAL_FILE)) {
                List<FlushResult> totalIncreFileList = levelFileMap.get(INCREMENTAL_FILE);
                int index = 0;
                while (index < totalIncreFileList.size()) {
                    long batchFileSize = 0;
                    List<FlushResult> batchFileList = new ArrayList<>();
                    while (index < totalIncreFileList.size() && batchFileList.size() < readFileNumLimit) {
                        FlushResult curFile = totalIncreFileList.get(index);
                        batchFileList.add(curFile);
                        batchFileSize += curFile.getFileSize();
                        index++;
                        if (batchFileSize > batchIncrementalFileSizeLimit) {
                            break;
                        }
                    }
                    initializeReader(batchFileList);
                    initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                    HashMap<String, List<FlushResult>> outFile = readAndWrite();
                    this.close();
                    if (outFile == null || outFile.size() < 1) {
                        LOG.info("incremental level compaction task: read file list is {}", batchFileList);
                        LOG.info("incremental level compaction task: after compaction out file info: " + outFile);
                        throw new IllegalStateException("incremental level compaction task: after compaction, without out file info, read file list is: " + batchFileList);
                    }
                    for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                        partitionKey = entry.getKey();
                        levelFileMap.computeIfAbsent(COMPACT_DIR, COMPACT_DIR -> new ArrayList<>()).addAll(entry.getValue());
                    }
                }
            }
            List<FlushResult> oriCompactFileList = levelFileMap.get(COMPACT_DIR);
            List<FlushResult> discardFileList = new ArrayList<>();
            if (oriCompactFileList.size() >= compactionReadFileNumberLimit) {
                int index = 0;
                List<FlushResult> curMergeList = new ArrayList<>();
                while (index < oriCompactFileList.size()) {
                    if (oriCompactFileList.get(index).getFileSize() < compactionReadFileSize || curMergeList.size() > 0) {
                        curMergeList.add(oriCompactFileList.get(index));
                        discardFileList.add(oriCompactFileList.get(index));
                    } else {
                        resultList.add(oriCompactFileList.get(index));
                    }

                    if (curMergeList.size() == 2) {
                        initializeReader(curMergeList);
                        initializeWriter(String.format("%s/%s", this.tablePath, COMPACT_DIR));
                        HashMap<String, List<FlushResult>> outFile = readAndWrite();
                        this.close();
                        if (outFile == null || outFile.size() < 1) {
                            LOG.info("COMPACT_DIR level compaction task: read file list is {}", curMergeList);
                            LOG.info("COMPACT_DIR level compaction task: after compaction out file info: " + outFile);
                            throw new IllegalStateException("COMPACT_DIR level compaction task: after compaction, without out file info, read file list is: " + curMergeList);
                        }
                        for (Map.Entry<String, List<FlushResult>> entry : outFile.entrySet()) {
                            partitionKey = entry.getKey();
                            resultList.addAll(entry.getValue());
                        }
                        curMergeList.clear();
                    }
                    index++;
                }
                if (!curMergeList.isEmpty()) {
                    resultList.addAll(curMergeList);
                    discardFileList.removeAll(curMergeList);
                }
                rsMap.put(partitionKey, resultList);
                rsMap.put(DISCARD_FILE_LIST_KEY, discardFileList);
            } else {
                rsMap.put(partitionKey, oriCompactFileList);
            }
        }
        if (partitionKey != null) {
            return rsMap;
        } else {
            return new HashMap<>();
        }
    }

    @Override
    public void close() throws Exception {
        this.nativeIOReader.close();
        this.nativeWriter.close();
    }
}
