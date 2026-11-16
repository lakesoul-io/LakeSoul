// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry.clean;

import com.dmetasoul.lakesoul.meta.JsonUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.data.Envelope;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class PgCleanDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
            throws Exception {
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        if (fields.length < 3) return;

        String tableName = fields[2];
        Struct value = (Struct) sourceRecord.value();
        if (value == null) return;
        ObjectNode result = JsonUtils.createObjectNode();
        ObjectNode beforeJson = extractStructJson(value.getStruct("before"));
        ObjectNode afterJson = extractStructJson(value.getStruct("after"));
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);

        result.put("commitOp", operation.toString().toLowerCase());
        result.put("tableName", tableName);
        // Retention for discard_compressed_file_info must start when the file was marked
        // discarded (the transaction commit time), not when it was originally written.
        Struct source = value.getStruct("source");
        if (source != null) {
            Object tsMs = source.get("ts_ms");
            if (tsMs instanceof Number) {
                result.put("eventTime", ((Number) tsMs).longValue());
            }
            Object snapshot = source.get("snapshot");
            result.put(
                    "snapshot",
                    snapshot != null
                            && ("true".equalsIgnoreCase(snapshot.toString())
                                    || "last".equalsIgnoreCase(snapshot.toString())));
        }
        if (tableName.equals("table_info")) {
            boolean hasPartitionTtlProperty = false;
            if (beforeJson.has("properties") && operation.toString().equalsIgnoreCase("delete")) {
                String tableId = beforeJson.path("table_id").asText(null);
                ObjectNode deleteJson = JsonUtils.createObjectNode();
                deleteJson.put("tableId", tableId);
                deleteJson.put("operation", "delete");
                result.set("after", deleteJson);
            }
            if (afterJson.has("properties")) {
                String afterProperties = afterJson.path("properties").asText();
                ObjectNode afterPropertiesParse = JsonUtils.parseObject(afterProperties);
                String tableId = afterJson.path("table_id").asText(null);
                afterPropertiesParse.put("tableId", tableId);
                result.set("after", afterPropertiesParse);
                hasPartitionTtlProperty = afterPropertiesParse.has("partition.ttl");
            }
            if (hasPartitionTtlProperty || operation.toString().equalsIgnoreCase("delete")) {
                collector.collect(result.toString());
            }
        } else {
            result.set("before", beforeJson);
            result.set("after", afterJson);
            if (!beforeJson.isEmpty() || !afterJson.isEmpty()) {
                collector.collect(result.toString());
            }
        }
    }

    private ObjectNode extractStructJson(Struct struct) {
        ObjectNode json = JsonUtils.createObjectNode();
        if (struct == null) return json;
        Schema schema = struct.schema();
        for (Field field : schema.fields()) {
            Object value = struct.get(field);
            if (value == null) {
                json.putNull(field.name());
                continue;
            }

            if ("file_ops".equals(field.name()) && value instanceof ArrayList) {
                JsonUtils.put(json, field.name(), convertByteBufferList((ArrayList<?>) value));
            } else {
                JsonUtils.put(json, field.name(), value);
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
