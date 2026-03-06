// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.entry;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.bson.*;
import org.bson.types.Decimal128;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MongoSinkUtils {

    public BsonValue convertTonBsonValue(Object value, List<String> structNameFiledList) throws ParseException {
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
                bsonArray.add(convertTonBsonValue(element, structNameFiledList));
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
                bsonDocument.append(name, convertTonBsonValue(fieldValue, structNameFiledList));
            }
            return bsonDocument;
        } else {
            throw new IllegalArgumentException("Unsupported data type: " + value.getClass());
        }
    }

    public List<String> traverseRow(RowType rowType) {
        List<String> nameList = new ArrayList<>();
        traverseField(rowType, nameList);
        return nameList;
    }

    private void traverseField(RowType rowType, List<String> nameList) {
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            String fieldName = rowType.getFieldNames().get(i);
            LogicalType fieldType = rowType.getTypeAt(i);
            nameList.add(fieldName);
            if (fieldType instanceof RowType) {
                traverseField((RowType) fieldType, nameList);
            }
        }
    }

    public void createMongoColl(String database, String collName, String uri) {
        MongoClient mongoClient = MongoClients.create(uri);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        mongoDatabase.createCollection(collName);
        mongoClient.close();
    }

    public List<String> findDirectNestedNames(JSONObject jsonObject, String targetFieldName, int currentLevel, int targetLevel) {
        List<String> nestedNames = new ArrayList<>();
        findDirectNestedNamesHelper(jsonObject, targetFieldName, currentLevel, targetLevel, nestedNames);
        return nestedNames;
    }

    public void findDirectNestedNamesHelper(JSONObject jsonObject, String targetFieldName, int currentLevel, int targetLevel, List<String> nestedNames) {
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

    public boolean isDateTimeString(Object value) {
        return value.toString().matches("^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$");
    }

    public Date parseDateTime(String value) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.parse(value);
    }

}