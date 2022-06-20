package org.apache.flink.lakesoul.tools;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import javax.annotation.Nullable;
import java.io.Serializable;



public class LakeSoulKeyGen implements Serializable {

    private  RowType rowType;
    private  Configuration conf;
    private  int rowKeyIndex;


    private LakeSoulKeyGen(RowType rowType , Configuration conf) {
        this.rowType =rowType;
        this.conf =conf;
        rowKeyIndex = getRowKeyIndex();
    }

    public static LakeSoulKeyGen instance(RowType rowType,Configuration conf){
        return new LakeSoulKeyGen(rowType,conf);
    }

    public  String getAndCheckRecordKey(Configuration conf, RowType rowType, RowData row) throws NoSuchFieldException {
        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
        String[] split = key.split(",");
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(rowType.getChildren().get(rowKeyIndex), rowKeyIndex);
        //TODO: more key not support
        return recordKey(fieldGetter.getFieldOrNull(row), split[0]);

    }
    public  String getAndCheckRecordKey(RowData row) throws NoSuchFieldException {
        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
        String[] split = key.split(",");
        RowData.FieldGetter fieldGetter = RowData.createFieldGetter(rowType.getChildren().get(rowKeyIndex), rowKeyIndex);
        //TODO: more key not support
        return recordKey(fieldGetter.getFieldOrNull(row), split[0]);
    }

    public  String recordKey(Object recordKeyValue, String recordKeyField) throws NoSuchFieldException {
        String recordKey = objToString(recordKeyValue);
        if (recordKey == null || recordKey.isEmpty()) {
            throw new NoSuchFieldException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
        }
        return recordKey;
    }

    public  String objToString(@Nullable Object obj) {
        return obj == null ? null : obj.toString();
    }

    public  int getRowKeyIndex(){
        String key = conf.getString(LakeSoulTableOptions.KEY_FIELD);
        String[] split = key.split(",");
        if (split.length==1) {
            return rowType.getFieldNames().indexOf(split[0]);
        }
        //TODO: more key
        return 0;

    }


}
