package org.apache.flink.lakesoul.test.connector.sink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.sink.LakeSoulMultiTableSinkStreamBuilder;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;
import static org.apache.flink.lakesoul.types.LakeSoulRecordConvert.setCDCRowKindField;

public class LakeSoulDataStreamSinkCase extends AbstractTestBase {

    @Test
    public void testLakeSoulDataStreamSink() throws Exception {
        new LakeSoulCatalog().cleanForTest();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);
        Configuration conf = new Configuration();
        boolean useCDC = true;
        conf.set(LakeSoulSinkOptions.USE_CDC, useCDC);
        conf.set(LakeSoulSinkOptions.IS_MULTI_TABLE_SOURCE, true);

        List<BinarySourceRecord> recordCollection = new ArrayList<>();
        String topic = "";
        List<String> primaryKeys = Arrays.asList("id");
        List<String> partitionKeys = Collections.emptyList();
        String tableName = "TestBinarySourceRecordSink";
        String path = getTempDirUri(tableName);
        TableId tableId = new TableId(LakeSoulCatalog.CATALOG_NAME, "default", tableName);
        LakeSoulRowDataWrapper data = mockInsertLakeSoulRowDataWrapper(1, useCDC, tableId);
        recordCollection.add(new BinarySourceRecord(topic, primaryKeys, tableId, path, partitionKeys, false, data, ""));
        Thread.sleep(100);
//        LakeSoulRowDataWrapper data1 = mockUpdateLakeSoulRowDataWrapper(2, useCDC, tableId);
        LakeSoulRowDataWrapper data1 = mockInsertLakeSoulRowDataWrapper(4, useCDC, tableId);
        recordCollection.add(new BinarySourceRecord(topic, primaryKeys, tableId, path, partitionKeys, false, data1, ""));

        DataStreamSource<BinarySourceRecord> source = env.fromCollection(recordCollection).setParallelism(1);
        LakeSoulRecordConvert lakeSoulRecordConvert = new LakeSoulRecordConvert(conf, conf.getString(SERVER_TIME_ZONE));

        MySqlSource<BinarySourceRecord> mySqlSource = MySqlSource.<BinarySourceRecord>builder()
                .deserializer(new BinaryDebeziumDeserializationSchema(lakeSoulRecordConvert,
                        conf.getString(WAREHOUSE_PATH)))
                .build();
        LakeSoulMultiTableSinkStreamBuilder.Context context = new LakeSoulMultiTableSinkStreamBuilder.Context();
        context.env = env;
        context.conf = conf;
        LakeSoulMultiTableSinkStreamBuilder
                builder =
                new LakeSoulMultiTableSinkStreamBuilder(mySqlSource, context, lakeSoulRecordConvert);
        builder.buildLakeSoulDMLSink(source);
        env.execute("test BinarySourceRecord Sink with CollectionSource");
    }

    private LakeSoulRowDataWrapper mockInsertLakeSoulRowDataWrapper(int seed, boolean useCDC, TableId tableId) {
        LakeSoulRowDataWrapper.Builder builder = LakeSoulRowDataWrapper
                .newBuilder()
                .setTableId(tableId)
                .setAfterType(genRowType(seed, useCDC))
                .setAfterRowData(genRowData(seed, useCDC, RowKind.INSERT))
                .setSrcTsMs(System.currentTimeMillis());
        return builder.build();
    }

    private LakeSoulRowDataWrapper mockUpdateLakeSoulRowDataWrapper(int seed, boolean useCDC, TableId tableId) {

        LakeSoulRowDataWrapper.Builder builder = LakeSoulRowDataWrapper
                .newBuilder()
                .setTableId(tableId)
                .setBeforeRowType(genRowType(seed - 1, useCDC))
                .setBeforeRowData(genRowData(seed - 1, useCDC, RowKind.UPDATE_BEFORE))
                .setAfterType(genRowType(seed, useCDC))
                .setAfterRowData(genRowData(seed, useCDC, RowKind.UPDATE_AFTER))
                .setSrcTsMs(System.currentTimeMillis());
        return builder.build();
    }

    public static RowType genRowType(int seed, boolean useCDC) {
        int arity;
        if (seed % 2 == 1) {
            arity = 2;
        } else {
            arity = 3;
        }
        arity += 1;
        if (useCDC) ++arity;


        String[] colNames = new String[arity];
        LogicalType[] colTypes = new LogicalType[arity];
        colNames[0] = "id";
        colTypes[0] = new IntType(false);
        if (seed % 2 == 1) {
            colNames[1] = "int";
            colTypes[1] = new IntType();
        } else {
            colNames[1] = "int";
            colTypes[1] = new IntType();
            colNames[2] = "str";
            colTypes[2] = new VarCharType();
        }

        colNames[useCDC ? arity - 2 : arity - 1] = SORT_FIELD;
        colTypes[useCDC ? arity - 2 : arity - 1] = new BigIntType();
        if (useCDC) {
            colNames[arity - 1] = CDC_CHANGE_COLUMN_DEFAULT;
            colTypes[arity - 1] = new VarCharType(false, Integer.MAX_VALUE);
        }
        return RowType.of(colTypes, colNames);
    }

    public static RowData genRowData(int seed, boolean useCDC, RowKind rowKind) {
        int arity;
        if (seed % 2 == 1) {
            arity = 2;
        } else {
            arity = 3;
        }
        arity += 1;
        if (useCDC) ++arity;
        // construct RowData
        BinaryRowData rowdata = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(rowdata);

        writer.writeInt(0, seed / 2);
        writer.writeInt(1, seed);
        if (seed % 2 == 0) {
            writer.writeString(2, StringData.fromString(String.valueOf(seed)));
        }
        long sortField = seed;
        writer.writeLong(useCDC ? arity - 2 : arity - 1, sortField);
        writer.writeRowKind(rowKind);

        if (useCDC) {
            setCDCRowKindField(writer, rowKind, arity - 1);
        }
        writer.complete();
        return rowdata;
    }
}
