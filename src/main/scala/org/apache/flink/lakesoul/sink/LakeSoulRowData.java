package org.apache.flink.lakesoul.sink;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.LakesoulCdcPartitionComputer;
import org.apache.flink.lakesoul.tools.FlinkUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;


public class LakeSoulRowData implements Serializable {



    private String recordKey;
    private String partitionKey;
    private RowData row;
//    private RowType rowType;

    public LakeSoulRowData(String key, String partitionKey, RowData row
//            ,RowType rowType
    ) {
        this.recordKey = key;
        this.partitionKey = partitionKey;
        this.row = row;
//        this.rowType=rowType;
    }

    public LakeSoulRowData() {
    }

    public static LakeSoulRowData transform(Configuration conf, RowType rowType, RowData row, LakesoulCdcPartitionComputer parComputer) throws NoSuchFieldException {
        String partitionKey = FlinkUtil.generatePartitionPath(
                parComputer.generatePartValues(row));
        String recordKey = getAndCheckRecordKey(conf, rowType, row);
        return new LakeSoulRowData(recordKey,partitionKey,row);
    }

    public static String getAndCheckRecordKey(Configuration conf,RowType rowType,RowData row) throws NoSuchFieldException {
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

    public String getRecordKey() {
        return recordKey;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public RowData getRow() {
        return row;
    }

    public void setRecordKey(String recordKey) {
        this.recordKey = recordKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public void setRow(RowData row) {
        this.row = row;
    }

//    public RowType getRowType() {
//        return rowType;
//    }

    public static String objToString(@Nullable Object obj) {
        return obj == null ? null : obj.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LakeSoulRowData that = (LakeSoulRowData) o;
        return Objects.equals(recordKey, that.recordKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordKey);
    }
}
