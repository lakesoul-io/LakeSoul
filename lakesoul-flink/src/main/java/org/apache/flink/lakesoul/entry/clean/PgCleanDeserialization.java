package org.apache.flink.lakesoul.entry.clean;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class PgCleanDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        if (fields.length < 3) return;

        String tableName = fields[2];
        Struct value = (Struct) sourceRecord.value();
        if (value == null) return;

        JSONObject result = new JSONObject();
        JSONObject beforeJson = extractStructJson(value.getStruct("before"));
        JSONObject afterJson = extractStructJson(value.getStruct("after"));

        if (!tableName.equals("partition_info") && !tableName.equals("discard_compressed_file_info")) {
            return;
        }
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("commitOp", operation.toString().toLowerCase());
        result.put("tableName", tableName);
        result.put("before", beforeJson);
        result.put("after", afterJson);

        if (!beforeJson.isEmpty() || !afterJson.isEmpty()) {
            collector.collect(result.toJSONString());
        }
    }

    private JSONObject extractStructJson(Struct struct) {
        JSONObject json = new JSONObject();
        if (struct == null) return json;

        Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            Object value = struct.get(field);
            if (value == null) {
                json.put(field.name(), null);
                continue;
            }

            if ("file_ops".equals(field.name()) && value instanceof ArrayList) {
                json.put(field.name(), convertByteBufferList((ArrayList<?>) value));
            } else {
                json.put(field.name(), value);
            }
        }
        return json;
    }

    private ArrayList<byte[]> convertByteBufferList(ArrayList<?> list) {
        ArrayList<byte[]> result = new ArrayList<>();
        for (Object obj : list) {
            if (obj instanceof ByteBuffer) {
                result.add(((ByteBuffer) obj).array());
            }
        }
        return result;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
