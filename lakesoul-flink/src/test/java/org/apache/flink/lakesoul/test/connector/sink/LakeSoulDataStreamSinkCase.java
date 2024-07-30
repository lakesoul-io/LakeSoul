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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;
import static org.apache.flink.lakesoul.types.LakeSoulRecordConvert.setCDCRowKindField;

public class LakeSoulDataStreamSinkCase extends AbstractTestBase {

    @Test
    public void testCollectionSource() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().registerTypeWithKryoSerializer(BinarySourceRecord.class, BinarySourceRecordSerializer.class);
        Configuration conf = new Configuration();
        boolean useCDC = true;
        conf.set(LakeSoulSinkOptions.USE_CDC, useCDC);
        conf.set(LakeSoulSinkOptions.IS_MULTI_TABLE_SOURCE, true);

        List<BinarySourceRecord> recordCollection = new ArrayList<>();
        String topic = "";
        List<String> primaryKeys = Collections.emptyList();
        List<String> partitionKeys = Collections.emptyList();
        String tableName = "TestBinarySourceRecordSink";
        String path = getTempDirUri(tableName);
        TableId tableId = new TableId(LakeSoulCatalog.CATALOG_NAME, "default", tableName);
        LakeSoulRowDataWrapper data = mockLakeSoulRowDataWrapper(0, useCDC, tableId);

        recordCollection.add(new BinarySourceRecord(topic, primaryKeys, tableId, path, partitionKeys, false, data, ""));

        DataStreamSource<BinarySourceRecord> source = env.fromCollection(recordCollection);
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

    private LakeSoulRowDataWrapper mockLakeSoulRowDataWrapper(int seed, boolean useCDC, TableId tableId) {
        int arity = 1 + 1;
        if (useCDC) ++arity;

        // construct RowData
        BinaryRowData rowdata = new BinaryRowData(arity);
        BinaryRowWriter writer = new BinaryRowWriter(rowdata);
        writer.writeInt(0, seed);

        RowKind rowKind = RowKind.INSERT;
        long sortField = 1;
        writer.writeLong(useCDC ? arity - 2 : arity - 1, sortField);
        writer.writeRowKind(rowKind);
        if (useCDC) {
            setCDCRowKindField(writer, rowKind, arity - 1);
        }
        writer.complete();

        // construct RowType
        String[] colNames = new String[arity];
        LogicalType[] colTypes = new LogicalType[arity];
        colNames[0] = "int";
        colTypes[0] = new IntType();

        colNames[useCDC ? arity - 2 : arity - 1] = SORT_FIELD;
        colTypes[useCDC ? arity - 2 : arity - 1] = new BigIntType();
        if (useCDC) {
            colNames[arity - 1] = CDC_CHANGE_COLUMN;
            colTypes[arity - 1] = new VarCharType(false, Integer.MAX_VALUE);
        }

        RowType rowType = RowType.of(colTypes, colNames);

        LakeSoulRowDataWrapper.Builder builder = LakeSoulRowDataWrapper
                .newBuilder()
                .setTableId(tableId)
                .setAfterType(rowType)
                .setAfterRowData(rowdata);
        return builder.build();
    }
}
