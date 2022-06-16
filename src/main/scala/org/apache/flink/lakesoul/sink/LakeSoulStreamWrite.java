package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.lakesoul.sink.Parquet.RowDataParquetWriteSupport;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;


public class LakeSoulStreamWrite <IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>{
    private LakeSoulKeyGen keyGen;
    private RowType rowType;
    private  MapState<String, RowData> nowData;

    private transient MapState<String, RowData> newData;
    private transient MapState<String, RowData> tmpData;

    public LakeSoulStreamWrite(LakeSoulKeyGen keyGen,RowType rowType) {
        this.keyGen = keyGen;
        this.rowType=rowType;
    }


    @Override
    public void processElement(StreamRecord<IN> streamRecord) throws Exception {
        RowData value = (RowData)streamRecord.getValue();
        //TODO : last data first
        nowData.put(keyGen.getRecordKey(value), value);

    }

    @Override
    public void open() throws Exception {
        MapStateDescriptor<String, RowData> nowDataDescriptor = new MapStateDescriptor<>("nowData", String.class, RowData.class);
        MapStateDescriptor<String, RowData> newDataDescriptor = new MapStateDescriptor<>("newData", String.class, RowData.class);
        MapStateDescriptor<String, RowData> tmpDataDescriptor = new MapStateDescriptor<>("tmpData", String.class, RowData.class);
        this.nowData = getRuntimeContext().getMapState(nowDataDescriptor);
        this.newData = getRuntimeContext().getMapState(newDataDescriptor);
        this.tmpData = getRuntimeContext().getMapState(tmpDataDescriptor);
    }

    @Override
    public void finish() throws Exception {

        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        tmpData=nowData;
        nowData=newData;

    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {


        HashMap<String, PriorityQueue<RowData>> partitionMap = new HashMap<>();
        if (nowData.isEmpty()){
            return;
        }
        nowData.entries().forEach(v->{
            String partitionKey = keyGen.getPartitionKey(v.getValue());
            if (partitionMap.containsKey(partitionKey)){
                partitionMap.get(partitionKey).add(v.getValue());
            }else {
                partitionMap.put(partitionKey, new PriorityQueue<>(Comparator.comparing(row-> {
                    try {
                        return keyGen.getRecordKey(row);
                    } catch (NoSuchFieldException e) {
                        e.printStackTrace();
                    }
                    return "null";
                })));
                partitionMap.get(partitionKey).add(v.getValue());
            }
        });
        partitionMap.forEach((k,v)->{
            System.out.println(k);
        });
        partitionMap.forEach((k,v)->{
            WriteSupport writeSupport = new RowDataParquetWriteSupport(rowType);
            try {
                LakeSoulRowDataParquetWriter writer = new LakeSoulRowDataParquetWriter(
                        new Path("d"),writeSupport, CompressionCodecName.LZ4,10000,10000);
                while(!v.isEmpty()){
                    writer.write(v.poll());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });



    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);
    }
}
