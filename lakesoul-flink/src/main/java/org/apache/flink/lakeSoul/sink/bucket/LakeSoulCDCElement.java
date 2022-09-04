package org.apache.flink.lakeSoul.sink.bucket;

import org.apache.flink.table.data.RowData;

public class LakeSoulCDCElement {
    public RowData element;
    public long timedata;
    public LakeSoulCDCElement(RowData rd, long td){
        this.element=rd;
        this.timedata=td;
    }
}
