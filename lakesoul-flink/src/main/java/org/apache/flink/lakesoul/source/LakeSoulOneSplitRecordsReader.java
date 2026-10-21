// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BATCH_SIZE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.LIMIT;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.dmetasoul.lakesoul.meta.DBUtil;

import io.substrait.proto.Plan;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector.Bytes;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

public class LakeSoulOneSplitRecordsReader implements RecordsWithSplitIds<RowData>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulOneSplitRecordsReader.class);

    private final LakeSoulPartitionSplit split;

    private final Configuration conf;

    // requested schema of the sql query
    private final RowType projectedRowType;

    // schema to pass to native reader
    private final RowType projectedRowTypeWithPk;

    private final long skipRecords;

    private final Set<String> finishedSplit;
    private final Schema partitionSchema;

    // Field getters for the requested (output) row type, used only by the
    // null-mismatch diagnostics below.
    private final RowData.FieldGetter[] projectedFieldGetters;

    // Index of the CDC row-kind column in projectedRowTypeWithPk, -1 if absent.
    private final int cdcFieldIndex;

    private static final int MAX_MISMATCH_LOGS = 30;
    private final AtomicLong mismatchLogCount = new AtomicLong();

    List<String> pkColumns;

    LinkedHashMap<String, String> partitionValues;

    boolean isBounded;

    String cdcColumn;

    RowData.FieldGetter cdcFieldGetter;

    private String splitId;

    private LakeSoulArrowReader reader;

    private VectorSchemaRoot currentVCR;

    // record index in current arrow batch (currentVCR)
    private int curRecordIdx = 0;

    // arrow batch -> row, returned by native reader
    private ArrowReader curArrowReader;

    // arrow batch -> row, with requested schema
    private ArrowReader curArrowReaderRequestedSchema;

    private boolean warnedNullRowKind = false;

    private final Plan filter;

    private long totalRead = 0;
    private long startTime;
    private long limit = Long.MAX_VALUE;

    public LakeSoulOneSplitRecordsReader(
            Configuration conf,
            LakeSoulPartitionSplit split,
            RowType tableRowType,
            RowType projectedRowType,
            RowType projectedRowTypeWithPk,
            List<String> pkColumns,
            boolean isBounded,
            String cdcColumn,
            List<String> partitionColumns,
            Plan filter)
            throws Exception {
        this.split = split;
        this.skipRecords = split.getSkipRecord();
        this.conf = new Configuration(conf);
        this.projectedRowType = projectedRowType;
        this.projectedRowTypeWithPk = projectedRowTypeWithPk;
        this.pkColumns = pkColumns;
        this.splitId = split.splitId();
        this.isBounded = isBounded;
        this.cdcColumn = cdcColumn;
        this.cdcFieldIndex =
                cdcColumn.isEmpty() ? -1 : projectedRowTypeWithPk.getFieldIndex(cdcColumn);
        this.projectedFieldGetters = new RowData.FieldGetter[projectedRowType.getFieldCount()];
        for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
            this.projectedFieldGetters[i] =
                    RowData.createFieldGetter(projectedRowType.getTypeAt(i), i);
        }
        this.finishedSplit = Collections.singleton(splitId);
        this.startTime = System.currentTimeMillis();
        Schema tableSchema = ArrowUtils.toArrowSchema(tableRowType);
        List<Field> partitionFields =
                partitionColumns.stream().map(tableSchema::findField).collect(Collectors.toList());

        this.partitionSchema = new Schema(partitionFields);
        this.partitionValues = DBUtil.parsePartitionDesc(split.getPartitionDesc());
        this.filter = filter;
        this.limit = conf.getLong(LIMIT, LIMIT.defaultValue());
        initializeReader();
        recoverFromSkipRecord();
    }

    private void initializeReader() throws IOException {
        NativeIOReader reader = new NativeIOReader();
        for (Path path : split.getFiles()) {
            reader.addFile(FlinkUtil.makeQualifiedPath(path).toString());
        }

        if (!projectedRowTypeWithPk.getChildren().isEmpty()) {
            ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
            // native reader requires pk columns in schema
            Schema arrowSchema = ArrowUtils.toArrowSchema(projectedRowTypeWithPk);
            reader.setSchema(arrowSchema);
            reader.setPrimaryKeys(pkColumns);
            FlinkUtil.setIOConfigs(conf, reader);
        }

        reader.setPartitionSchema(partitionSchema);
        reader.setBatchSize(conf.get(BATCH_SIZE));

        if (cdcFieldIndex >= 0) {
            cdcFieldGetter = RowData.createFieldGetter(new VarCharType(), cdcFieldIndex);
        }

        for (Map.Entry<String, String> partition : this.partitionValues.entrySet()) {
            reader.setDefaultColumnValue(partition.getKey(), partition.getValue());
        }

        if (filter != null) {
            reader.addFilterProto(this.filter);
        }

        LOG.info(
                "Initializing reader for split {}, pk={}, partitions={},"
                        + " actual read cols={}, cdc column={}, filter={}",
                split,
                pkColumns,
                partitionValues,
                projectedRowTypeWithPk,
                cdcColumn,
                filter);
        try {
            reader.initializeReader();
        } catch (Throwable t) {
            LOG.error("Failed to initialize LakeSoul Reader for split: {}", split, t);
            try {
                reader.close();
            } catch (Exception e) {
                t.addSuppressed(e);
            }
            if (t instanceof IOException) throw (IOException) t;
            throw new IOException("Initialize native reader failed", t);
        }
        this.reader = new LakeSoulArrowReader(reader, 10000);
        LOG.info(
                "Initialized reader for split {}, time {}ms",
                split,
                System.currentTimeMillis() - startTime);
    }

    // final returned row should only contain requested schema in query
    private void makeCurrentArrowReader() {
        this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.projectedRowTypeWithPk);
        // this.schema contains only requested fields, which does not include cdc column
        // and may not include pk columns
        ArrayList<FieldVector> requestedVectors = new ArrayList<>();
        for (String fieldName : projectedRowType.getFieldNames()) {
            int index = projectedRowTypeWithPk.getFieldIndex(fieldName);
            requestedVectors.add(currentVCR.getVector(index));
        }
        this.curArrowReaderRequestedSchema =
                ArrowUtils.createArrowReader(
                        new VectorSchemaRoot(requestedVectors), projectedRowType);
        if (LOG.isInfoEnabled()) {
            int requestedRows =
                    requestedVectors.isEmpty() ? 0 : requestedVectors.get(0).getValueCount();
            LOG.info(
                    "makeCurrentArrowReader split={} vcrRows={} requestedRows={} curRecordIdx={}"
                        + " totalRead={} vcrFields={} requestedFields={} cdcField={} cdcColumn={}",
                    split,
                    currentVCR.getRowCount(),
                    requestedRows,
                    curRecordIdx,
                    totalRead,
                    currentVCR.getSchema().getFields().size(),
                    projectedRowType.getFieldCount(),
                    cdcFieldIndex,
                    cdcColumn);
        }
    }

    private void recoverFromSkipRecord() throws Exception {
        LOG.info(
                "recoverFromSkipRecord start split={} skipRecords={} limit={} currentVCRRows={}"
                        + " curRecordIdx={} totalRead={}",
                split,
                skipRecords,
                limit,
                currentVCR == null ? -1 : currentVCR.getRowCount(),
                curRecordIdx,
                totalRead);
        if (skipRecords > 0) {
            long skipRowCount = 0;
            while (skipRowCount <= skipRecords) {
                boolean hasNext = this.reader.hasNext();
                if (!hasNext) {
                    close();
                    String error =
                            String.format(
                                    "Encounter unexpected EOF in split=%s, skipRecords=%s,"
                                            + " skipRowCount=%s",
                                    split, skipRecords, skipRowCount);
                    LOG.error(error);
                    throw new IOException(error);
                }
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                skipRowCount += this.currentVCR.getRowCount();
                LOG.info(
                        "recoverFromSkipRecord batch split={} rows={} skipRowCount={}"
                                + " skipRecords={} curRecordIdx={} totalRead={}",
                        split,
                        this.currentVCR.getRowCount(),
                        skipRowCount,
                        skipRecords,
                        curRecordIdx,
                        totalRead);
            }
            skipRowCount -= currentVCR.getRowCount();
            curRecordIdx = (int) (skipRecords - skipRowCount);
            totalRead = skipRecords;
        } else {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                curRecordIdx = 0;
            } else {
                close();
                return;
            }
        }
        makeCurrentArrowReader();
        LOG.info(
                "recoverFromSkipRecord done split={} vcrRows={} curRecordIdx={} totalRead={}"
                        + " limit={} skipRecords={} reader={}",
                split,
                currentVCR == null ? -1 : currentVCR.getRowCount(),
                curRecordIdx,
                totalRead,
                limit,
                skipRecords,
                reader);
    }

    private void logNullMismatch(RowData rd, int rowId) {
        if (!LOG.isWarnEnabled() || mismatchLogCount.get() >= MAX_MISMATCH_LOGS) {
            return;
        }
        long afterRecovery = totalRead - skipRecords;
        if (afterRecovery > 200 && (totalRead % 10000) != 0) {
            return;
        }
        ColumnVector[] columns = curArrowReaderRequestedSchema.getColumnVectors();
        for (int i = 0; i < rd.getArity(); i++) {
            boolean nullFlag;
            Object value;
            try {
                nullFlag = rd.isNullAt(i);
                value = projectedFieldGetters[i].getFieldOrNull(rd);
            } catch (Throwable t) {
                if (mismatchLogCount.incrementAndGet() <= MAX_MISMATCH_LOGS) {
                    LOG.warn(
                            "NULL_MISMATCH read error split={} rowId={} curRecordIdx={} vcrRows={}"
                                    + " totalRead={} field={} type={}",
                            split,
                            rowId,
                            curRecordIdx,
                            currentVCR == null ? -1 : currentVCR.getRowCount(),
                            totalRead,
                            i,
                            projectedRowType.getTypeAt(i),
                            t);
                }
                continue;
            }
            if (nullFlag == (value == null)) {
                continue;
            }
            long n = mismatchLogCount.incrementAndGet();
            if (n > MAX_MISMATCH_LOGS) {
                return;
            }
            String colClass = i < columns.length ? columns[i].getClass().getName() : "?";
            String colNull = i < columns.length ? String.valueOf(columns[i].isNullAt(rowId)) : "?";
            String colBytes = "?";
            try {
                if (i < columns.length && columns[i] instanceof BytesColumnVector) {
                    Bytes bytes = ((BytesColumnVector) columns[i]).getBytes(rowId);
                    colBytes =
                            bytes == null
                                    ? "null"
                                    : "data="
                                            + (bytes.data == null ? "null" : bytes.data.length)
                                            + ",offset="
                                            + bytes.offset
                                            + ",len="
                                            + bytes.len;
                }
            } catch (Throwable t) {
                colBytes = "ERR:" + t;
            }
            LOG.warn(
                    "NULL_MISMATCH split={} rowId={} curRecordIdx={} vcrRows={} totalRead={}"
                            + " field={} type={} nullFlag={} value={} valueClass={} rowClass={}"
                            + " colClass={} colNull={} colBytes={}",
                    split,
                    rowId,
                    curRecordIdx,
                    currentVCR == null ? -1 : currentVCR.getRowCount(),
                    totalRead,
                    i,
                    projectedRowType.getTypeAt(i),
                    nullFlag,
                    value,
                    value == null ? "null" : value.getClass().getName(),
                    rd.getClass().getName(),
                    colClass,
                    colNull,
                    colBytes);
            if (n >= MAX_MISMATCH_LOGS) {
                return;
            }
        }
    }

    @Nullable
    @Override
    public String nextSplit() {
        String nextSplit = this.splitId;
        this.splitId = null;
        return nextSplit;
    }

    @Nullable
    @Override
    public RowData nextRecordFromSplit() {
        if (reader == null || currentVCR == null) {
            LOG.info("No next record available, reader {}, vector root {}", reader, currentVCR);
            return null;
        }
        while (true) {
            if (totalRead >= this.limit) {
                this.reader.close();
                LOG.info("Reach limit condition {}", split);
                this.reader = null;
                return null;
            }
            if (curRecordIdx >= currentVCR.getRowCount()) {
                if (this.reader.hasNext()) {
                    this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                    makeCurrentArrowReader();
                    curRecordIdx = 0;
                    LOG.info(
                            "nextRecordFromSplit new batch split={} rows={} curRecordIdx={}"
                                    + " totalRead={} limit={}",
                            split,
                            currentVCR.getRowCount(),
                            curRecordIdx,
                            totalRead,
                            limit);
                } else {
                    this.reader.close();
                    this.reader = null;
                    LOG.info("Reach end of split file {}", split);
                    return null;
                }
            }

            RowData rd = null;
            RowKind rk = RowKind.INSERT;
            int rowId = 0;

            while (curRecordIdx < currentVCR.getRowCount()) {
                rowId = curRecordIdx;
                curRecordIdx++;
                // row kind by default is insert
                rd = this.curArrowReader.read(rowId);
                if (!cdcColumn.isEmpty()) {
                    if (!this.isBounded) {
                        // set rowkind according to cdc row kind field value;
                        // keep INSERT when the CDC operation is missing/unknown
                        // (e.g. rows produced by a non-CDC backfill).
                        RowKind parsed =
                                FlinkUtil.operationToRowKind(
                                        (StringData) cdcFieldGetter.getFieldOrNull(rd));
                        if (parsed == null) {
                            if (!warnedNullRowKind) {
                                LOG.warn(
                                        "Row kind of cdc is null/unknown for split {}, defaulting"
                                                + " to INSERT (further occurrences at debug)",
                                        split);
                                warnedNullRowKind = true;
                            }
                            LOG.debug("Row kind of cdc is null/unknown, defaulting to INSERT");
                            rk = RowKind.INSERT;
                        } else {
                            rk = parsed;
                        }
                        LOG.debug("Set RowKind to {}", rk);
                    } else {
                        if (FlinkUtil.isCDCDelete((StringData) cdcFieldGetter.getFieldOrNull(rd))) {
                            // batch read from cdc table should filter delete rows
                            rd = null;
                            continue;
                        }
                    }
                }
                break;
            }

            if (rd == null) {
                continue;
            }

            if (rk == null) {
                LOG.warn("Row kind of cdc is null for split {}, defaulting to INSERT", split);
                rk = RowKind.INSERT;
            }

            // we have get one valid row, return row with requested schema
            rd = this.curArrowReaderRequestedSchema.read(rowId);
            // change rowkind if needed
            rd.setRowKind(rk);
            logNullMismatch(rd, rowId);
            totalRead++;
            if (totalRead % 1000000 == 0) {
                LOG.info("Read {} records from split {}", totalRead, split);
            }
            return rd;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        LOG.info("Finished splits {}", finishedSplit);
        return finishedSplit;
    }

    @Override
    public void close() throws Exception {
        long endTime = System.currentTimeMillis();
        LOG.info(
                "Close reader split {}, read num {}, time {}ms",
                split,
                totalRead,
                endTime - startTime);
        if (this.currentVCR != null) {
            this.currentVCR.close();
            this.currentVCR = null;
        }
        if (this.reader != null) {
            this.reader.close();
            this.reader = null;
        }
    }
}
