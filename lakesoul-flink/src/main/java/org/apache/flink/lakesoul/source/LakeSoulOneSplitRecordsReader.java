package org.apache.flink.lakesoul.source;

import org.apache.arrow.lakesoul.io.NativeIOReader;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
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
    private RowType fileSchema;

    private LakeSoulArrowReader reader;

    private VectorSchemaRoot currentVCR;

    private int curRecordId = 0;

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


    public LakeSoulOneSplitRecordsReader(Configuration conf, LakeSoulSplit split, RowType schema, List<String> pkColumns) throws IOException {
        this.split = split;
        this.conf = conf;
        this.schema = schema;
        this.pkColumns = pkColumns;
        this.splitId = split.splitId();
        initializeReader();
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
            List<RowType.RowField> fields = schema.getFields().stream().filter(field -> !partitionCols.contains(field.getName())).collect(Collectors.toList());
            tmp = new RowType(fields);
            this.partitionIndexes = Arrays.stream(partitionCols.toArray()).mapToInt(columnList::indexOf).toArray();
            this.partitionTypes = Arrays.stream(partitionIndexes).mapToObj(columnTypeList::get).toArray(LogicalType[]::new);
            this.partitionFieldGetters = IntStream.range(0, partitionTypes.length).mapToObj(i -> RowData.createFieldGetter(partitionTypes[i], partitionIndexes[i])).toArray(RowData.FieldGetter[]::new);
        } else {
            tmp = this.schema;
        }
        this.fileSchema = tmp;

        List<Integer> partitionIndexList = Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        this.nonPartitionIndexes = IntStream.range(0, columnList.size()).filter(c -> !partitionIndexList.contains(c)).toArray();
        this.nonPartitionTypes = Arrays.stream(nonPartitionIndexes).mapToObj(columnTypeList::get).toArray(LogicalType[]::new);
        this.nonPartitionFieldGetters = IntStream.range(0, nonPartitionTypes.length).mapToObj(i -> RowData.createFieldGetter(nonPartitionTypes[i], i)).toArray(RowData.FieldGetter[]::new);

        Schema arrowSchema = ArrowUtils.toArrowSchema(fileSchema);
        reader.setSchema(arrowSchema);
        //FlinkUtil.setFSConfigs(conf, reader);
        reader.initializeReader();
        this.reader = new LakeSoulArrowReader(reader, 10000);
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

        if (this.currentVCR == null) {
            if (this.reader.hasNext()) {
                this.currentVCR = this.reader.nextResultVectorSchemaRoot();
                this.curArrowReader = ArrowUtils.createArrowReader(currentVCR, this.schema);

                if (this.currentVCR == null) {
                    return null;
                }
                curRecordId = 0;
            } else {
                this.reader.close();
                return null;
            }
        }

        if (curRecordId < currentVCR.getRowCount()) {
            int tmp = curRecordId;
            curRecordId++;
            RowData rd = this.curArrowReader.read(tmp);
            GenericRowData reuseRow = new GenericRowData(this.schema.getFieldCount());
            for (int i = 0; i < nonPartitionIndexes.length; i++) {
                reuseRow.setField(nonPartitionIndexes[i], nonPartitionFieldGetters[i].getFieldOrNull(rd));
            }
            for (int j=0; j < partitionIndexes.length; j++) {
                reuseRow.setField(partitionIndexes[j],FlinkUtil.convertStringToInternalValue(partitions.get(columnList.get(partitionIndexes[j])),partitionTypes[j]));
            }
            return reuseRow;
        } else {
            return null;
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return Collections.singleton(split.splitId());
    }
}
