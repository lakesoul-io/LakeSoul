package org.apache.flink.lakesoul.tools;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.sink.partition.LakesoulCdcPartitionComputer;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;


public class LakeSoulKeyGen implements Serializable {

//
//    private static LakesoulCdcPartitionComputer parComputer;
//    private static RowType rowType;
//    private static Configuration conf;
//    private static int rowKeyIndex;
//    private static final String DEFAULT_PAR_NAME = "";
//
//
//    private LakeSoulKeyGen(RowType rowType , DataType dataType, List<String> partitionKeys, Boolean isCdc,Configuration conf) {
//        parComputer=getPartitionComputer(dataType,partitionKeys,isCdc);
//        LakeSoulKeyGen.rowType =rowType;
//        LakeSoulKeyGen.conf =conf;
//        rowKeyIndex = getRowKeyIndex();
//    }
//
//    public static LakeSoulKeyGen instance(RowType rowType,DataType dataType, List<String> partitionKeys, Boolean isCdc,Configuration conf){
//        System.out.println("hh====================================");
//        return new LakeSoulKeyGen(rowType,dataType,partitionKeys,isCdc,conf);
//    }
//
//
//    public String getPartitionKey(RowData row){
//        return FlinkUtil.generatePartitionPath(
//                parComputer.generatePartValues(row));
//    }
//
//    public String getRecordKey(RowData row) throws NoSuchFieldException {
//           return getAndCheckRecordKey(conf, rowType, row);
//    }
//
//    public static String getAndCheckRecordKey(Configuration conf, RowType rowType, RowData row) throws NoSuchFieldException {
//        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
//        String[] split = key.split(",");
//        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(rowType.getChildren().get(rowKeyIndex), rowKeyIndex);
//        //TODO: more key not support
//        return recordKey(fieldGetter.getFieldOrNull(row), split[0]);
//
//    }
//    public static String recordKey(Object recordKeyValue, String recordKeyField) throws NoSuchFieldException {
//        String recordKey = objToString(recordKeyValue);
//        if (recordKey == null || recordKey.isEmpty()) {
//            throw new NoSuchFieldException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
//        }
//        return recordKey;
//    }
//
//    public static String objToString(@Nullable Object obj) {
//        return obj == null ? null : obj.toString();
//    }
//
//
//    public  SortCodeGenerator createSortCodeGenerator() {
//        SortSpec.SortSpecBuilder builder = SortSpec.builder();
//        builder.addField(rowKeyIndex, true, true);
//        return new SortCodeGenerator(new TableConfig(), rowType, builder.build());
//    }
//
//    public static int getRowKeyIndex(){
//        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
//        String[] split = key.split(",");
//        if (split.length==1) {
//            return rowType.getFieldNames().indexOf(split[0]);
//        }
//        //TODO: more key
//        return 0;
//
//    }
//
//    public static LakesoulCdcPartitionComputer getPartitionComputer(DataType physicalRowDataType,List<String> partitionKeys, Boolean isCdc){
//        return new LakesoulCdcPartitionComputer(
//                DEFAULT_PAR_NAME,
//                getFieldNames(physicalRowDataType).toArray(new String[0]),
//                getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
//                partitionKeys.toArray(new String[0]), isCdc);
//
//    }
//
//
//
//    public static List<String> getFieldNames(DataType dataType) {
//        final LogicalType type = dataType.getLogicalType();
//        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE  ) {
//            return getFieldNames(dataType.getChildren().get(0));
//        } else if (isCompositeType(type)) {
//            return LogicalTypeChecks.getFieldNames(type);
//        }
//        return Collections.emptyList();
//    }
//
//    public static List<DataType> getFieldDataTypes(DataType dataType) {
//        final LogicalType type = dataType.getLogicalType();
//        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE  ) {
//            return getFieldDataTypes(dataType.getChildren().get(0));
//        } else if (isCompositeType(type)) {
//            return dataType.getChildren();
//        }
//        return Collections.emptyList();
//    }
}
