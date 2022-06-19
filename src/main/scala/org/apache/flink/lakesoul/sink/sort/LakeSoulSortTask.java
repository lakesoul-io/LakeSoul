package org.apache.flink.lakesoul.sink.sort;


import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.sort.BinaryExternalSorter;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowDataSerializer;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.MutableObjectIterator;
import java.util.HashMap;


//This feature will be supported in the next version

public class LakeSoulSortTask extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, BoundedOneInput {


    private GeneratedNormalizedKeyComputer gComputer;
    private GeneratedRecordComparator gComparator;
    private transient BinaryExternalSorter sorter;
    private transient StreamRecordCollector<RowData> collector;
    private transient BinaryRowDataSerializer binarySerializer;
    private HashMap<String, Integer> buckets;
    private LakeSoulKeyGen keyGen;

    public LakeSoulSortTask(
            GeneratedNormalizedKeyComputer gComputer, GeneratedRecordComparator gComparator,LakeSoulKeyGen keyGen) {
        this.gComputer = gComputer;
        this.gComparator = gComparator;
        this.keyGen = keyGen;
    }


    @Override
    public void open() throws Exception {
        super.open();

        buckets=new HashMap<>();
        ClassLoader cl = getContainingTask().getUserCodeClassLoader();

        AbstractRowDataSerializer inputSerializer = (AbstractRowDataSerializer) getOperatorConfig().getTypeSerializerIn1(getUserCodeClassloader());
        this.binarySerializer = new BinaryRowDataSerializer(inputSerializer.getArity());

        NormalizedKeyComputer computer = gComputer.newInstance(cl);
        RecordComparator comparator = gComparator.newInstance(cl);
        gComputer = null;
        gComparator = null;

        MemoryManager memManager = getContainingTask().getEnvironment().getMemoryManager();
        this.sorter =
                new BinaryExternalSorter(
                        this.getContainingTask(),
                        memManager,
                        computeMemorySize(),
                        this.getContainingTask().getEnvironment().getIOManager(),
                        inputSerializer,
                        binarySerializer,
                        computer,
                        comparator,
                        getContainingTask().getJobConfiguration());
        this.sorter.startThreads();

        collector = new StreamRecordCollector<>(output);


    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
//        String partitionKey = keyGen.getPartitionKey(element.getValue());
//        if (buckets.containsKey(partitionKey)){
//            int nowNumber = buckets.get(partitionKey);
//            //TODO  File block size rules
//            if (nowNumber>10){
//                this.sorter.write(element.getValue());
//                endInput();
//                buckets.put(partitionKey,0);
//                return;
//            }
//        }else {
//            buckets.put(partitionKey,0);
//        }
//        this.sorter.write(element.getValue());
//        buckets.put(partitionKey,buckets.get(partitionKey)+1);
    }


    @Override
    public void endInput() throws Exception {
        BinaryRowData row = binarySerializer.createInstance();
        MutableObjectIterator<BinaryRowData> iterator = sorter.getIterator();
        while ((row = iterator.next(row)) != null) {
            collector.collect(row);
        }
    }



    @Override
    public void close() throws Exception {
        super.close();
        if (sorter != null) {
            sorter.close();
        }
    }


}
