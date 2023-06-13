/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.apache.flink.table.utils.PartitionPathUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LakeSoulOneSplitRecordsReader implements RecordsWithSplitIds<RowData> {

    private final LakeSoulSplit split;
    private final Configuration conf;

    // requested schema of the sql query
    private final RowType schema;

    // schema to pass to native reader
    private final RowType schemaWithPk;
    private final long skipRecords;
    List<String> pkColumns;
    LinkedHashMap<String, String> partitions;
    boolean isStreaming;
    String cdcColumn;
    RowData.FieldGetter cdcFieldGetter;
    long skipRowCount = 0;
    private String splitId;
    private LakeSoulArrowReader reader;
    private VectorSchemaRoot currentVCR;
    private int curRecordId = 0;

    // arrow batch -> row, returned by native reader
    private ArrowReader curArrowReader;

    // arrow batch -> row, with requested schema
    private ArrowReader curArrowReaderRequestedSchema;

    public LakeSoulOneSplitRecordsReader(Configuration conf, LakeSoulSplit split, RowType schema, RowType schemaWithPk,
                                         List<String> pkColumns, boolean isStreaming, String cdcColumn)
            throws IOException {
        this.split = split;
        this.skipRecords = split.getSkipRecord();
        this.conf = new Configuration(conf);
        this.schema = schema;
        this.schemaWithPk = schemaWithPk;
        this.pkColumns = pkColumns;
        this.splitId = split.splitId();
        this.isStreaming = isStreaming;
        this.cdcColumn = cdcColumn;
        initializeReader();
        recoverFromSkipRecord();
    }

    private void initializeReader() throws IOException {
        NativeIOReader reader = new NativeIOReader();
        for (Path path : split.getFiles()) {
            reader.addFile(FlinkUtil.makeQualifiedPath(path).toString());
        }
        this.partitions = PartitionPathUtils.extractPartitionSpecFromPath(split.getFiles().get(0));

        List<String> nonPartitionColumns =
                this.schema.getFieldNames().stream().filter(name -> !this.partitions.containsKey(name))
                        .collect(Collectors.toList());

        if (!nonPartitionColumns.isEmpty()) {
            ArrowUtils.setLocalTimeZone(FlinkUtil.getLocalTimeZone(conf));
            // native reader requires pk columns in schema
            Schema arrowSchema = ArrowUtils.toArrowSchema(schemaWithPk);
            reader.setSchema(arrowSchema);
            reader.setPrimaryKeys(pkColumns);
            FlinkUtil.setFSConfigs(conf, reader);
        }

        for (Map.Entry<String, String> partition : this.partitions.entrySet()) {
            reader.setDefaultColumnValue(partition.getKey(), partition.getValue());
        }

        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader, 10000);
    }

    // final returned row should only contain requested schema in query
    private void makeCurrentArrowReader() {
        this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.schemaWithPk);
        // this.schema contains only requested fields, which does not include cdc column
        // and may not include pk columns
        ArrayList<FieldVector> requestedVectors = new ArrayList<>();
        for (String fieldName : schema.getFieldNames()) {
            int index = schemaWithPk.getFieldIndex(fieldName);
            requestedVectors.add(currentVCR.getVector(index));
        }
        this.curArrowReaderRequestedSchema =
                ArrowUtils.createArrowReader(new VectorSchemaRoot(requestedVectors), schema);
    }

    private void recoverFromSkipRecord() {
        if (skipRecords > 0) {
            while (skipRowCount <= skipRecords && this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                skipRowCount += this.currentVCR.getRowCount();
            }
            skipRowCount -= currentVCR.getRowCount();
            curRecordId = (int) (skipRecords - skipRowCount);
        } else {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                curRecordId = 0;
            } else {
                this.reader.close();
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
        while (true) {
            if (curRecordId >= currentVCR.getRowCount()) {
                if (this.reader.hasNext()) {
                    this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                    makeCurrentArrowReader();
                    curRecordId = 0;
                } else {
                    this.reader.close();
                    return null;
                }
            }

            RowData rd = null;
            int rowId = 0;

            while (curRecordId < currentVCR.getRowCount()) {
                rowId = curRecordId;
                curRecordId++;
                rd = this.curArrowReader.read(rowId);
                if (!"".equals(this.cdcColumn)) {
                    if (this.isStreaming) {
                        rd.setRowKind(FlinkUtil.operationToRowKind((StringData) cdcFieldGetter.getFieldOrNull(rd)));
                    } else {
                        if (FlinkUtil.isCDCDelete((StringData) cdcFieldGetter.getFieldOrNull(rd))) {
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

            // we have get one valid row, unnecessary fields and return the row
            return this.curArrowReaderRequestedSchema.read(rowId);
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return Collections.singleton(split.splitId());
    }
}
