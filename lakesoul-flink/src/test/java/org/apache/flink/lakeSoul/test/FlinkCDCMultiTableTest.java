package org.apache.flink.lakeSoul.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.lakeSoul.types.SourceRecordJsonSerde;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils.*;
import static com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils.discoverCapturedTableSchemas;

public class FlinkCDCMultiTableTest {

    static List<Tuple2<TableId, TableChanges.TableChange>> getTableSchemaFromSource(MySqlSourceConfig sourceConfig) {
        try (MySqlConnection jdbc = createMySqlConnection(sourceConfig.getDbzConfiguration())) {
            final Map<TableId, TableChanges.TableChange> tables = discoverCapturedTableSchemas(sourceConfig, jdbc);
            return tables.entrySet().stream().map(e -> Tuple2.of(e.getKey(), e.getValue())).collect(Collectors.toList());
        } catch (Exception e) {
            throw new FlinkRuntimeException(
                    "Failed to discover captured tables for enumerator", e);
        }
    }

    public static class ForwardDeserializeSchema
            implements DebeziumDeserializationSchema<SourceRecord> {

        private static final long serialVersionUID = 2975058057832211228L;

        @Override
        public void deserialize(SourceRecord record, Collector<SourceRecord> out) {
            out.collect(record);
        }

        @Override
        public TypeInformation<SourceRecord> getProducedType() {
            return TypeInformation.of(SourceRecord.class);
        }
    }

    public static class JsonDebeziumDeserializationSchema implements DebeziumDeserializationSchema<JsonSourceRecord> {
        private static final long serialVersionUID = 1L;

        public JsonDebeziumDeserializationSchema() {}

        public void deserialize(SourceRecord record, Collector<JsonSourceRecord> out) {
            out.collect(JsonSourceRecord.fromKafkaSourceRecord(record, SourceRecordJsonSerde.getInstance()));
        }

        public TypeInformation<JsonSourceRecord> getProducedType() {
            return TypeInformation.of(new TypeHint<>() {
            });
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(3000);
        env.getConfig().disableForceKryo();
        env.getConfig().enableForceAvro();

//        final OutputTag<String> outputTag = new OutputTag<>("ddl-side-output");

        MySqlSource<JsonSourceRecord> mySqlSource = MySqlSource.<JsonSourceRecord>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test_cdc") // set captured database
                .tableList("test_cdc.*") // set captured table
                .username("root")
                .password("root")
                .scanNewlyAddedTableEnabled(true)
                .includeSchemaChanges(true)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


//        MySqlSourceConfigFactory configFactory = mySqlSource.getConfigFactory();
//        MySqlSourceConfig sourceConfig = configFactory.createConfig(0);
//
//        // 初始化表 schema 流
//        DataStreamSource<Tuple2<TableId, TableChanges.TableChange>> schemaSource =
//                env.fromCollection(getTableSchemaFromSource(sourceConfig));
//
        // CDC + CreateTable + Alter Table 流
        DataStreamSource<JsonSourceRecord> source = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1);
//        source.addSink(new DiscardingSink<>());
//
//        // 将原始 stream 拆分为 CDC 和 DDL 两个流
//
//        SingleOutputStreamOperator<String> stream = source.process(new ProcessFunction<>() {
//            @Override
//            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) {
//                JSONObject json = JSON.parseObject(value);
//                if (json.containsKey("historyRecord")) {
//                    // emit data to side output
//                    ctx.output(outputTag, value);
//                } else {
//                    out.collect(value);
//                }
//            }
//        });
//
//        DataStream<String> ddlStream = stream.getSideOutput(outputTag);
//
//        // 将 DDL 流转换为包含表名、主键
//
//
//
//
//        // a map descriptor to store the name of the rule (string) and the rule itself.
//        MapStateDescriptor<, TableSchemaValue> schemaStateDescriptor = new MapStateDescriptor<>(
//                "RulesBroadcastState",
//                TypeInformation.of(new TypeHint<TableSchemaKey>() {}),
//                TypeInformation.of(new TypeHint<TableSchemaValue>() {}));
//
//
        DataStreamSink<JsonSourceRecord> sink = source.print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
