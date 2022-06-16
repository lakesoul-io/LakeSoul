package org.apache.flink.lakesoul.tools;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.LakesoulCdcPartitionComputer;
import org.apache.flink.lakesoul.sink.LakeSoulTableOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

import static org.apache.flink.lakesoul.sink.LakeSoulSink.getPartitionComputer;

public class LakeSoulKeyGen implements Serializable {


    private static LakesoulCdcPartitionComputer parComputer;
    private static RowType rowType;
    private static DataType dataType;
    private static Configuration conf;

    private LakeSoulKeyGen(RowType rowType , DataType dataType, List<String> partitionKeys, Boolean isCdc,Configuration conf) {
        parComputer=getPartitionComputer(dataType,partitionKeys,isCdc);
        LakeSoulKeyGen.dataType =dataType;
        LakeSoulKeyGen.rowType =rowType;
        LakeSoulKeyGen.conf =conf;
    }

    public static LakeSoulKeyGen instance(RowType rowType,DataType dataType, List<String> partitionKeys, Boolean isCdc,Configuration conf){
       return new LakeSoulKeyGen(rowType,dataType,partitionKeys,isCdc,conf);
    }


    public String getPartitionKey(RowData row){
        return FlinkUtil.generatePartitionPath(
                parComputer.generatePartValues(row));
    }

    public String getRecordKey(RowData row) throws NoSuchFieldException {
           return getAndCheckRecordKey(conf, rowType, row);
    }

    public static String getAndCheckRecordKey(Configuration conf, RowType rowType, RowData row) throws NoSuchFieldException {
        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
        String[] split = key.split(",");
        if (split.length==1){
            int keyIndex= rowType.getFieldNames().indexOf(split[0]);
            RowData.FieldGetter fieldGetter = RowData.createFieldGetter(rowType.getChildren().get(keyIndex), keyIndex);
            return recordKey(fieldGetter.getFieldOrNull(row), split[0]);
        }
        //TODO
        return "";
    }
    public static String recordKey(Object recordKeyValue, String recordKeyField) throws NoSuchFieldException {
        String recordKey = objToString(recordKeyValue);
        if (recordKey == null || recordKey.isEmpty()) {
            throw new NoSuchFieldException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
        }
        return recordKey;
    }

    public static String objToString(@Nullable Object obj) {
        return obj == null ? null : obj.toString();
    }


}
