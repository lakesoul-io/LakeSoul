// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.mongodb.sink.writer.context.MongoSinkContext;
import org.apache.flink.connector.mongodb.sink.writer.serializer.MongoSerializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.bson.*;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MongoSinkUtils {

    static Table coll;
    static List<String> structNameFiledList;

    public static void createMongoColl(String database, String collName, String uri) {
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        mongoDatabase.createCollection(collName);
        mongoClient.close();
    }

    public static class MyMongoSerializationSchema implements MongoSerializationSchema<Tuple2<Boolean, Row>>, Serializable {
        @Override
        public WriteModel<BsonDocument> serialize(Tuple2<Boolean, Row> record, MongoSinkContext context) {
            Row row = record.f1; // Extract the Row object from the Tuple2
            BsonDocument document = new BsonDocument();
            int fieldCount = row.getArity();
            DataType[] fieldDataTypes = coll.getSchema().getFieldDataTypes();
            for (int i = 0; i < fieldCount; i++) {
                String fieldName = coll.getSchema().getFieldNames()[i];
                Object fieldValue = row.getField(i);
                if (fieldValue instanceof Row) {
                    DataType dataType = fieldDataTypes[i];
                    RowType rowType = (RowType) dataType.getLogicalType();
                    structNameFiledList = traverseRow(rowType);
                }
                if (fieldValue != null) {
                    try {
                        document.append(fieldName, convertTonBsonValue(fieldValue));
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return new InsertOneModel<>(document);
        }


        public static BsonValue convertTonBsonValue(Object value) throws ParseException {
            if (value == null) {
                return new BsonNull();
            } else if (value instanceof Integer) {
                return new BsonInt32((Integer) value);
            } else if (value instanceof Long) {
                return new BsonInt64((Long) value);
            } else if (value instanceof String) {
                return new BsonString((String) value);
            } else if (value instanceof Boolean) {
                return new BsonBoolean((Boolean) value);
            } else if (value instanceof Double) {
                return new BsonDouble((Double) value);
            } else if (value instanceof BigDecimal) {
                return new BsonDecimal128(new Decimal128((BigDecimal) value));
            } else if (value instanceof Date) {
                return new BsonDateTime((long) value);
            } else if (value instanceof BinaryType) {
                return new BsonBinary((byte[]) value);
            } else if (value instanceof byte[]) {
                return new BsonBinary((byte[]) value);
            } else if (value instanceof Object[]) {
                Object[] array = (Object[]) value;
                BsonArray bsonArray = new BsonArray();
                for (Object element : array) {
                    bsonArray.add(convertTonBsonValue(element));
                }
                return bsonArray;
            } else if (isDateTimeString(value)) {
                Date date = parseDateTime(value.toString());
                return new BsonDateTime(date.getTime());
            } else if (value instanceof Row) {
                Row row = (Row) value;
                BsonDocument bsonDocument = new BsonDocument();
                for (int i = 0; i < row.getArity(); i++) {
                    Object fieldValue = row.getField(i);
                    List<String> stringList = new ArrayList<>(structNameFiledList);
                    String name = structNameFiledList.get(0);
                    stringList.remove(0);
                    structNameFiledList = stringList;
                    bsonDocument.append(name, convertTonBsonValue(fieldValue));
                }
                return bsonDocument;
            } else {
                throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
            }
        }

        public static List<String> traverseRow(RowType rowType) {
            List<String> nameList = new ArrayList<>();
            traverseField(rowType, nameList);
            return nameList;
        }

        private static void traverseField(RowType rowType, List<String> nameList) {
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                String fieldName = rowType.getFieldNames().get(i);
                LogicalType fieldType = rowType.getTypeAt(i);
                nameList.add(fieldName);
                if (fieldType instanceof RowType) {
                    traverseField((RowType) fieldType, nameList);
                }
            }
        }

        public static List<String> findDirectNestedNames(JSONObject jsonObject, String targetFieldName, int currentLevel, int targetLevel) {
            List<String> nestedNames = new ArrayList<>();
            findDirectNestedNamesHelper(jsonObject, targetFieldName, currentLevel, targetLevel, nestedNames);
            return nestedNames;
        }

        public static void findDirectNestedNamesHelper(JSONObject jsonObject, String targetFieldName, int currentLevel, int targetLevel, List<String> nestedNames) {
            if (currentLevel == targetLevel) {
                if (jsonObject.getString("name").equals(targetFieldName)) {
                    JSONArray children = jsonObject.getJSONArray("children");
                    for (Object obj : children) {
                        JSONObject child = (JSONObject) obj;
                        nestedNames.add(child.getString("name"));
                    }
                }
            } else {
                JSONArray children = jsonObject.getJSONArray("children");
                for (Object obj : children) {
                    JSONObject child = (JSONObject) obj;
                    findDirectNestedNamesHelper(child, targetFieldName, currentLevel + 1, targetLevel, nestedNames);
                }
            }
        }

        public static boolean isDateTimeString(Object value) {
            return value.toString().matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$");
        }

        public static Date parseDateTime(String value) throws ParseException {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat.parse(value);
        }
    }
}