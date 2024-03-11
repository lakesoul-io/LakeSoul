// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import org.bson.Document;

import java.util.Map;

public class ParseDocument {
    public static Struct convertBSONToStruct(String value) {
        Document bsonDocument = Document.parse(value);
        // 创建根 Struct 的 Schema
        SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
        Struct struct = new Struct(buildSchema(bsonDocument, structSchemaBuilder));
        // 填充 Struct 的值
        fillStructValues(bsonDocument, struct);

        return struct;
    }

    private static Schema buildSchema(Document bsonDocument, SchemaBuilder structSchemaBuilder) {
        for (Map.Entry<String, Object> entry : bsonDocument.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document) {
                // 处理嵌套的 Document
                SchemaBuilder nestedStructSchemaBuilder = SchemaBuilder.struct();
                structSchemaBuilder.field(fieldName, buildSchema((Document) value, nestedStructSchemaBuilder));
            } else {
                // 处理普通字段
                structSchemaBuilder.field(fieldName, getSchemaForValue(value));
            }
        }
        return structSchemaBuilder.build();
    }

    private static void fillStructValues(Document bsonDocument, Struct struct) {
        for (Map.Entry<String, Object> entry : bsonDocument.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document) {
                // 处理嵌套的 Document
                Struct nestedStruct = new Struct(struct.schema().field(fieldName).schema());
                fillStructValues((Document) value, nestedStruct);
                struct.put(fieldName, nestedStruct);
            } else {
                // 处理普通字段
                struct.put(fieldName, value);
            }
        }
    }

    private static Schema getSchemaForValue(Object value) {
        // 根据值的类型返回对应的 Schema
        if (value instanceof String) {
            return Schema.STRING_SCHEMA;
        } else if (value instanceof Integer) {
            return Schema.INT32_SCHEMA;
        } else if (value instanceof Long) {
            return Schema.INT64_SCHEMA;
        } else if (value instanceof Double) {
            return Schema.FLOAT64_SCHEMA;
        } else if (value instanceof Boolean) {
            return Schema.BOOLEAN_SCHEMA;
        } else if (value instanceof Byte) {
            return Schema.BYTES_SCHEMA;
        }  else {
            // 处理其他类型，可以根据实际情况添加更多类型
            return Schema.STRING_SCHEMA;
        }
    }
}
