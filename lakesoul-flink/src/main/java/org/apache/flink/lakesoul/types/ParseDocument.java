// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.*;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.List;

public class ParseDocument {
    public static Struct convertBSONToStruct(String value) {
        Document bsonDocument = Document.parse(value);
        SchemaBuilder structSchemaBuilder = SchemaBuilder.struct();
        Struct struct = new Struct(buildSchema(bsonDocument, structSchemaBuilder));
        fillStructValues(bsonDocument, struct);

        return struct;
    }

    private static Schema buildSchema(Document bsonDocument, SchemaBuilder structSchemaBuilder) {
        for (Map.Entry<String, Object> entry : bsonDocument.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document) {
                SchemaBuilder nestedStructSchemaBuilder = SchemaBuilder.struct();
                structSchemaBuilder.field(fieldName, buildSchema((Document) value, nestedStructSchemaBuilder));
            }  else if (value instanceof List) {
                List<?> arrayList = (List<?>) value;
                Schema arraySchema = getSchemaForArrayList(arrayList);
                structSchemaBuilder.field(fieldName, arraySchema);
            } else {
                structSchemaBuilder.field(fieldName, getSchemaForValue(value));
            }
        }
        return structSchemaBuilder.build();
    }

    private static Schema getSchemaForArrayList(List<?> arrayList) {
        Schema elementSchema = null;
        if (!arrayList.isEmpty()) {
            Object firstElement = arrayList.get(0);
            elementSchema = getSchemaForValue(firstElement);
        }
        return SchemaBuilder.array(elementSchema).build();
    }

    private static void fillStructValues(Document bsonDocument, Struct struct) {
        for (Map.Entry<String, Object> entry : bsonDocument.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof Document) {
                Struct nestedStruct = new Struct(struct.schema().field(fieldName).schema());
                fillStructValues((Document) value, nestedStruct);
                struct.put(fieldName, nestedStruct);
            } else if (value instanceof List) {
                List<?> arrayList = (List<?>) value;
                struct.put(fieldName, arrayList);
            } else if (value instanceof Decimal128) {
                BigDecimal decimalValue = new BigDecimal(value.toString());
                struct.put(fieldName, decimalValue);
            } else if (value instanceof Binary) {
                Binary binaryData = (Binary) value;
                struct.put(fieldName,binaryData.getData());
            } else if (value instanceof BsonTimestamp) {
                BsonTimestamp bsonTimestamp = (BsonTimestamp) value;
                struct.put(fieldName,bsonTimestamp.getValue());
            } else {
                struct.put(fieldName,value);
            }
        }
    }

    private static Schema getSchemaForValue(Object value) {
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
        } else if (value instanceof Decimal128) {
            BigDecimal decimalValue = new BigDecimal(value.toString());
            return Decimal.schema(decimalValue.scale());
        } else if (value instanceof Byte) {
            return Schema.BYTES_SCHEMA;
        } else if (value instanceof Binary) {
            return Schema.BYTES_SCHEMA;
        } else if (value instanceof Date) {
            return Timestamp.SCHEMA;
        } else if (value instanceof BsonTimestamp) {
            return Schema.INT64_SCHEMA;
        } else {
            return Schema.STRING_SCHEMA;
        }
    }
}
