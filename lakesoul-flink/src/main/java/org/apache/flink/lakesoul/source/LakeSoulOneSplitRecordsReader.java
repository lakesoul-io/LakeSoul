// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

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
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
    private final List<String> partitionColumns;
    private final RowType tableRowType;
    private final Schema partitionSchema;

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

    private final Plan filter;

    public LakeSoulOneSplitRecordsReader(Configuration conf,
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
        this.tableRowType = tableRowType;
        this.projectedRowType = projectedRowType;
        this.projectedRowTypeWithPk = projectedRowTypeWithPk;
        this.pkColumns = pkColumns;
        this.splitId = split.splitId();
        this.isBounded = isBounded;
        this.cdcColumn = cdcColumn;
        this.finishedSplit = Collections.singleton(splitId);
        this.partitionColumns = partitionColumns;
        Schema tableSchema = ArrowUtils.toArrowSchema(tableRowType);
        List<Field> partitionFields = partitionColumns.stream().map(tableSchema::findField).collect(Collectors.toList());

        this.partitionSchema = new Schema(partitionFields);
        this.partitionValues = DBUtil.parsePartitionDesc(split.getPartitionDesc());
        this.filter = filter;
        initializeReader();
        recoverFromSkipRecord();
    }

    private void initializeReader() throws IOException {
        NativeIOReader reader = new NativeIOReader();
        for (Path path : split.getFiles()) {
            reader.addFile(FlinkUtil.makeQualifiedPath(path).toString());
        }

        List<String> nonPartitionColumns =
                this.projectedRowType.getFieldNames().stream().filter(name -> !this.partitionValues.containsKey(name))
                        .collect(Collectors.toList());

        if (!nonPartitionColumns.isEmpty()) {
            ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
            // native reader requires pk columns in schema
            Schema arrowSchema = ArrowUtils.toArrowSchema(projectedRowTypeWithPk);
            reader.setSchema(arrowSchema);
            reader.setPrimaryKeys(pkColumns);
            FlinkUtil.setFSConfigs(conf, reader);
        }

        reader.setPartitionSchema(partitionSchema);

        if (!cdcColumn.isEmpty()) {
            int cdcField = projectedRowTypeWithPk.getFieldIndex(cdcColumn);
            cdcFieldGetter = RowData.createFieldGetter(new VarCharType(), cdcField);
        }

        for (Map.Entry<String, String> partition : this.partitionValues.entrySet()) {
            reader.setDefaultColumnValue(partition.getKey(), partition.getValue());
        }

        if (filter != null) {
            reader.addFilterProto(this.filter);
        }

        LOG.info("Initializing reader for split {}, pk={}, partitions={}," +
                        " non partition cols={}, cdc column={}, filter={}",
                split,
                pkColumns,
                partitionValues,
                nonPartitionColumns,
                cdcColumn,
                filter);
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader,
                10000);
    }

    // final returned row should only contain requested schema in query
    private void makeCurrentArrowReader() {
        this.curArrowReader = ArrowUtils.createArrowReader(currentVCR,
                this.projectedRowTypeWithPk);
        // this.schema contains only requested fields, which does not include cdc column
        // and may not include pk columns
        ArrayList<FieldVector> requestedVectors = new ArrayList<>();
        for (String fieldName : projectedRowType.getFieldNames()) {
            int index = projectedRowTypeWithPk.getFieldIndex(fieldName);
            requestedVectors.add(currentVCR.getVector(index));
        }
        this.curArrowReaderRequestedSchema =
                ArrowUtils.createArrowReader(new VectorSchemaRoot(requestedVectors), projectedRowType);
    }

    private void recoverFromSkipRecord() throws Exception {
        LOG.info("Recover from skip record={} for split={}", skipRecords, split);
        if (skipRecords > 0) {
            long skipRowCount = 0;
            while (skipRowCount <= skipRecords) {
                boolean hasNext = this.reader.hasNext();
                if (!hasNext) {
                    close();
                    String error =
                            String.format("Encounter unexpected EOF in split=%s, skipRecords=%s, skipRowCount=%s",
                                    split,
                                    skipRecords,
                                    skipRowCount);
                    LOG.error(error);
                    throw new IOException(error);
                }
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                skipRowCount += this.currentVCR.getRowCount();
            }
            skipRowCount -= currentVCR.getRowCount();
            curRecordIdx = (int) (skipRecords - skipRowCount);
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
        if (reader == null) {
            return null;
        }
        while (true) {
            if (curRecordIdx >= currentVCR.getRowCount()) {
                if (this.reader.hasNext()) {
                    this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                    makeCurrentArrowReader();
                    curRecordIdx = 0;
                } else {
                    this.reader.close();
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
                        // set rowkind according to cdc row kind field value
                        rk = FlinkUtil.operationToRowKind((StringData) cdcFieldGetter.getFieldOrNull(rd));
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

            // we have get one valid row, return row with requested schema
            rd = this.curArrowReaderRequestedSchema.read(rowId);
            // change rowkind if needed
            rd.setRowKind(rk);
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
