package org.apache.flink.lakeSoul.sink.bucket;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordComparator;

import java.io.Serializable;
import java.util.Comparator;

public class LakeSoulCDCComparator implements Comparator<LakeSoulCDCElement>, Serializable {
    private  RecordComparator rc;
    public LakeSoulCDCComparator(RecordComparator rc){
        this.rc=rc;
    }
    @Override
    public int compare(LakeSoulCDCElement E1, LakeSoulCDCElement E2) {
        int res=rc.compare(E1.element,E2.element);
        if(res!=0){
            return res;
        }else{
            res=compareLong(E1.timedata,E2.timedata);
            if(res!=0){
                return res;
            }else{
                return compareEvent(E1.element,E2.element);
            }
        }
    }
    public int compareLong(long e1,long e2){
        long res=e1-e2;
        if(res==0){
            return 0;
        }else{
            if(res<0){
                return -1;
            }else{
                return 1;
            }
        }
    }
    public int compareEvent(RowData e1,RowData e2){
        return Byte.compare(e1.getRowKind().toByteValue(),e2.getRowKind().toByteValue());
    }

}
