// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.bucket;

import org.apache.flink.connector.file.table.PartitionComputer;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.DateTimeUtils;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_EMPTY_STRING;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NULL_STRING;

public class CdcPartitionComputer implements PartitionComputer<RowData> {

    private static final long serialVersionUID = 1L;

    protected String defaultPartValue;
    protected String[] partitionColumns;
    protected int[] partitionIndexes;
    protected LogicalType[] partitionTypes;
    protected RowData.FieldGetter[] partitionFieldGetters;

    private final int[] nonPartitionIndexes;
    private final LogicalType[] nonPartitionTypes;
    protected RowData.FieldGetter[] nonPartitionFieldGetters;
    private final Boolean isCdc;
    private transient GenericRowData reuseRow;

    public CdcPartitionComputer(
            String defaultPartValue,
            String[] columnNames,
            RowType rowType,
            String[] partitionColumns, Boolean isCdc) {
        this(defaultPartValue, columnNames,
                rowType.getChildren(),
                partitionColumns, isCdc);
    }

    public CdcPartitionComputer(
            String defaultPartValue,
            String[] columnNames,
            DataType[] columnTypes,
            String[] partitionColumns, Boolean isCdc) {
        this(defaultPartValue, columnNames,
                Arrays.stream(columnTypes)
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList()),
                partitionColumns, isCdc);
    }

    public CdcPartitionComputer(
            String defaultPartValue,
            String[] columnNames,
            List<LogicalType> columnTypeList,
            String[] partitionColumns, Boolean isCdc) {
        this.defaultPartValue = defaultPartValue;
        this.isCdc = isCdc;
        List<String> columnList = Arrays.asList(columnNames);

        this.partitionColumns = partitionColumns;
        this.partitionIndexes =
                Arrays.stream(this.partitionColumns).mapToInt(columnList::indexOf).toArray();
        this.partitionTypes =
                Arrays.stream(partitionIndexes)
                        .mapToObj(columnTypeList::get)
                        .toArray(LogicalType[]::new);
        this.partitionFieldGetters =
                IntStream.range(0, partitionTypes.length)
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                partitionTypes[i], partitionIndexes[i]))
                        .toArray(RowData.FieldGetter[]::new);

        List<Integer> partitionIndexList =
                Arrays.stream(partitionIndexes).boxed().collect(Collectors.toList());
        this.nonPartitionIndexes =
                IntStream.range(0, columnNames.length)
                        .filter(c -> !partitionIndexList.contains(c))
                        .toArray();
        this.nonPartitionTypes =
                Arrays.stream(nonPartitionIndexes)
                        .mapToObj(columnTypeList::get)
                        .toArray(LogicalType[]::new);
        this.nonPartitionFieldGetters =
                IntStream.range(0, nonPartitionTypes.length)
                        .mapToObj(
                                i ->
                                        RowData.createFieldGetter(
                                                nonPartitionTypes[i], nonPartitionIndexes[i]))
                        .toArray(RowData.FieldGetter[]::new);
    }

    @Override
    public LinkedHashMap<String, String> generatePartValues(RowData in) {
        LinkedHashMap<String, String> partSpec = new LinkedHashMap<>();

        for (int i = 0; i < partitionIndexes.length; i++) {
            Object field = partitionFieldGetters[i].getFieldOrNull(in);
            String partitionValue;
            if (field == null) {
                partitionValue = LAKESOUL_NULL_STRING;
            } else if (partitionTypes[i].getTypeRoot() == LogicalTypeRoot.DATE) {
                // convert date to readable date string
                LocalDate d = LocalDate.ofEpochDay((Integer) field);
                partitionValue = d.toString();
            } else if (partitionTypes[i].getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE) {
                partitionValue = DateTimeUtils.formatTimestamp((TimestampData) field, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
            } else {
                partitionValue = field.toString();
                if ("".equals(partitionValue)) {
                    partitionValue = LAKESOUL_EMPTY_STRING;
                }
            }
            partSpec.put(partitionColumns[i], partitionValue);
        }
        return partSpec;
    }

    @Override
    public RowData projectColumnsToWrite(RowData in) {
        if (partitionIndexes.length == 0) {
            return in;
        }
        int len = nonPartitionIndexes.length;
        if (isCdc) {
            len += 1;
        }
        if (reuseRow == null) {
            this.reuseRow = new GenericRowData(len);
        }

        for (int i = 0; i < nonPartitionIndexes.length; i++) {
            reuseRow.setField(i, nonPartitionFieldGetters[i].getFieldOrNull(in));
        }
        if (isCdc) {
            reuseRow.setField(len - 1, FlinkUtil.rowKindToOperation(in.getRowKind()));
        }
        reuseRow.setRowKind(in.getRowKind());
        return reuseRow;
    }
}