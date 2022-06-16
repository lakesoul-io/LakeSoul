package org.apache.flink.lakesoul.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.LakesoulCdcPartitionComputer;
import org.apache.flink.lakesoul.tools.LakeSoulKeyGen;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isCompositeType;

public class LakeSoulSink {

    private static final String DEFAULT_PAR_NAME = "";

    private static LakesoulCdcPartitionComputer parComputer;

    public static DataStreamSink<RowData> partitionSortWrite(Configuration conf, RowType rowType,
                                                                     DataStream<RowData> dataStream, DataType physicalRowDataType,
                                                                     List<String> partitionKeys) {


        LakeSoulKeyGen keyGen = LakeSoulKeyGen.instance(rowType, physicalRowDataType, partitionKeys, true, conf);

        dataStream = dataStream.keyBy(keyGen::getPartitionKey);

        DataStream<RowData> writeStream = dataStream.transform("writeTas",
                TypeInformation.of(RowData.class),
                new LakeSoulStreamWrite<>(keyGen,rowType));


        return writeStream.addSink(new DiscardingSink<>())
                .name("end")
                .setParallelism(1);
    }




    public static List<String> getFieldNames(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE  ) {
            return getFieldNames(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return LogicalTypeChecks.getFieldNames(type);
        }
        return Collections.emptyList();
    }

    public static List<DataType> getFieldDataTypes(DataType dataType) {
        final LogicalType type = dataType.getLogicalType();
        if (type.getTypeRoot() == LogicalTypeRoot.DISTINCT_TYPE  ) {
            return getFieldDataTypes(dataType.getChildren().get(0));
        } else if (isCompositeType(type)) {
            return dataType.getChildren();
        }
        return Collections.emptyList();
    }
    public static LakesoulCdcPartitionComputer getPartitionComputer(DataType physicalRowDataType,List<String> partitionKeys, Boolean isCdc){
        return new LakesoulCdcPartitionComputer(
                DEFAULT_PAR_NAME,
                getFieldNames(physicalRowDataType).toArray(new String[0]),
                getFieldDataTypes(physicalRowDataType).toArray(new DataType[0]),
                partitionKeys.toArray(new String[0]), isCdc);

    }
}
