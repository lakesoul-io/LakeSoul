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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.PartitionPathUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class LakeSoulOneSplitRecordsReader implements RecordsWithSplitIds<RowData> {

    private final LakeSoulSplit split;

    @Nonnull
    private String splitId;

    private Configuration conf;

    private final RowType schema;

    private final RowType schemaWithPk;
    private RowType fileSchema;

    private LakeSoulArrowReader reader;

    private VectorSchemaRoot currentVCR;

    private int curRecordId = 0;
    private long skipRecords;
    private int totalRecords = 0;

    private ArrowReader curArrowReader;
    List<String> pkColumns;
    LinkedHashMap<String, String> partitions;
    List<String> columnList;

    private int[] partitionIndexes;
    private LogicalType[] partitionTypes;
    private RowData.FieldGetter[] partitionFieldGetters;
    private int[] nonPartitionIndexes;
    private LogicalType[] nonPartitionTypes;
    private RowData.FieldGetter[] nonPartitionFieldGetters;

    private boolean partitionsNon;
    boolean isStreaming;
    String cdcColumn;
    RowData.FieldGetter cdcFieldGetter;

    long skipRowCount = 0;

    public LakeSoulOneSplitRecordsReader(Configuration conf, LakeSoulSplit split, RowType schema, RowType schemaWithPk, List<String> pkColumns, boolean partitionsNon, boolean isStreaming, String cdcColumn) throws IOException {
        this.split = split;
        this.skipRecords = split.getSkipRecord();
        this.conf = new Configuration(conf);
        this.schema = schema;
        this.schemaWithPk = schemaWithPk;
        this.pkColumns = pkColumns;
        this.splitId = split.splitId();
        this.partitionsNon = partitionsNon;
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
        Set<String> partitionCols = this.partitions.keySet();
        this.columnList = this.schema.getFieldNames();
        List<LogicalType> columnTypeList = schema.getChildren();
        RowType tmp;
        if (null != partitionCols && partitionCols.size() > 0) {
            List<RowType.RowField> fields = schemaWithPk.getFields().stream().filter(field -> !partitionCols.contains(field.getName())).collect(Collectors.toList());
            tmp = new RowType(fields);
            this.partitionIndexes = Arrays.stream(partitionCols.toArray()).mapToInt(columnList::indexOf).toArray();
            if (columnTypeList.size() != 0) {
                this.partitionTypes = Arrays.stream(partitionIndexes).mapToObj(i -> i != -1 ? (columnTypeList.get(i)) : columnTypeList.get(0)).toArray(LogicalType[]::new);
                this.partitionFieldGetters = IntStream.range(0, partitionTypes.length).mapToObj(i -> RowData.createFieldGetter(partitionTypes[i], partitionIndexes[i])).toArray(RowData.FieldGetter[]::new);
            }
        } else {
            tmp = this.schemaWithPk;
        }
        this.fileSchema = tmp;
        if (!"".equals(this.cdcColumn)) {
            cdcFieldGetter = RowData.createFieldGetter(new VarCharType(), this.fileSchema.getFieldCount() - 1);
        }
        List<Integer> partitionIndexList;
        if (partitionIndexes == null || partitionIndexes.length == 0) {
            partitionIndexList = new ArrayList<>();
        } else {
            partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        }
        this.nonPartitionIndexes = IntStream.range(0, columnList.size()).filter(c -> !partitionIndexList.contains(c)).toArray();
        this.nonPartitionTypes = Arrays.stream(nonPartitionIndexes).mapToObj(columnTypeList::get).toArray(LogicalType[]::new);
        this.nonPartitionFieldGetters = IntStream.range(0, nonPartitionTypes.length).mapToObj(i -> RowData.createFieldGetter(nonPartitionTypes[i], i)).toArray(RowData.FieldGetter[]::new);
        if (nonPartitionIndexes.length != 0) {
            Schema arrowSchema = ArrowUtils.toArrowSchema(fileSchema);
            reader.setSchema(arrowSchema);
            reader.setPrimaryKeys(pkColumns);
            FlinkUtil.setFSConfigs(conf, reader);
            reader.initializeReader();
            this.reader = new LakeSoulArrowReader(reader, 10000);
        }
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
        this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.fileSchema);

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
        if (this.nonPartitionIndexes.length == 0) {
            if (curRecordId != 0 || this.partitionsNon) {
                return null;
            }
            GenericRowData reuseRow = new GenericRowData(this.schema.getFieldCount());
            setReuseRowWithPartition(reuseRow);
            curRecordId++;
            totalRecords++;
            if (skipRecords < totalRecords) {
                return reuseRow;
            } else {
                return null;
            }
        } else {
            if (curRecordId >= currentVCR.getRowCount()) {
                if (this.reader.hasNext()) {
                    this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                    this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.fileSchema);
                    curRecordId = 0;
                } else {
                    this.reader.close();
                    return null;
                }

            }
            GenericRowData reuseRow = null;
            while (curRecordId < currentVCR.getRowCount()) {
                int tmp = curRecordId;
                curRecordId++;
                totalRecords++;
                RowData rd = this.curArrowReader.read(tmp);
                reuseRow = new GenericRowData(this.schema.getFieldCount());
                for (int i = 0; i < nonPartitionIndexes.length; i++) {
                    reuseRow.setField(nonPartitionIndexes[i], nonPartitionFieldGetters[i].getFieldOrNull(rd));
                }
                if (!"".equals(this.cdcColumn)) {
                    if (this.isStreaming) {
                        reuseRow.setRowKind(FlinkUtil.operationToRowKind((StringData) cdcFieldGetter.getFieldOrNull(rd)));
                    } else {
                        if (FlinkUtil.isCDCDelete((StringData) cdcFieldGetter.getFieldOrNull(rd))) {
                            reuseRow = null;
                            continue;
                        }else{
                            setReuseRowWithPartition(reuseRow);
                            return reuseRow;
                        }
                    }
                }
                setReuseRowWithPartition(reuseRow);
                break;
            }
            return reuseRow;
        }
    }

    private void setReuseRowWithPartition(GenericRowData reuseRow) {
        if (partitionIndexes != null && partitionIndexes.length != 0) {
            for (int j = 0; j < partitionIndexes.length; j++) {
                if (partitionIndexes[j] != -1) {
                    reuseRow.setField(partitionIndexes[j], FlinkUtil.convertStringToInternalValue(partitions.get(columnList.get(partitionIndexes[j])), partitionTypes[j]));
                }
            }
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return Collections.singleton(split.splitId());
    }
}
