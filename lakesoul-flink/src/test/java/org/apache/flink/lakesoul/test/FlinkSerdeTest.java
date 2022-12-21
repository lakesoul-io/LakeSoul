package org.apache.flink.lakesoul.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.JsonDebeziumDeserializationSchema;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

public class FlinkSerdeTest {

    static public class BinarySourceRecord implements Serializable {

        public BinarySourceRecord(BinaryRowData rowData, RowType rowType) {
            this.rowData = rowData;
            this.rowType = rowType;
        }

        BinaryRowData rowData;
        RowType rowType;

        @Override
        public String toString() {
            return "BinarySourceRecord{" +
                    "rowData=" + rowData +
                    ", rowType=" + rowType.asSerializableString() +
                    '}';
        }
    }

    static class BinarySourceRecordDeserSchema implements DebeziumDeserializationSchema<BinarySourceRecord> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<BinarySourceRecord> collector) throws Exception {
            BinaryRowData rowData = new BinaryRowData(2);
            BinaryRowWriter writer = new BinaryRowWriter(rowData);
            writer.writeInt(0, 999);
            writer.writeString(1, StringData.fromString("test string"));
            writer.complete();
            RowType rowType = new RowType(Arrays.asList(
                    new RowType.RowField("int_col", new IntType()),
                    new RowType.RowField("string_col", new VarCharType())
                    ));
            collector.collect(new BinarySourceRecord(rowData, rowType));
        }

        @Override
        public TypeInformation<BinarySourceRecord> getProducedType() {
            return TypeInformation.of(new TypeHint<BinarySourceRecord>() {
            });
        }
    }

    @Test
    public void Test() throws Exception {
        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);
        env.getConfig().registerTypeWithKryoSerializer(RowType.class, JavaSerializer.class);

        MySqlSourceBuilder<BinarySourceRecord> sourceBuilder = MySqlSource.<BinarySourceRecord>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test_cdc") // set captured // database
                .tableList("test_cdc.*") // set captured table
                .username("root")
                .password("root")
                .includeSchemaChanges(true)
                .scanNewlyAddedTableEnabled(true)
                .serverTimeZone("UTC")
                .deserializer(new BinarySourceRecordDeserSchema());
        MySqlSource<BinarySourceRecord> mySqlSource = sourceBuilder.build();

        DataStreamSource<BinarySourceRecord> source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);
        source.addSink(new PrintSinkFunction<>());
        env.execute("test");
    }
}
