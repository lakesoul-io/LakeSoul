// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.flink.lakesoul.entry;


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
import java.util.List;

public class PgDeserialization implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        JSONObject result = new JSONObject();
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        String database = fields[1];
        String tableName = fields[2];
        Struct value = (Struct) sourceRecord.value();

        // get "before" data
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema beforeSchema = before.schema();
            List<Field> beforeFields = beforeSchema.fields();
            for (Field beforeField : beforeFields) {
                Object beforeValue = before.get(beforeField);
                String name = beforeField.name();
                if (name.equals("file_ops")){
                    ArrayList afterValue1 = (ArrayList) before.get(beforeField);
                    Object[] objects = afterValue1.toArray();
                    ArrayList<byte[]> arrayList = new ArrayList();
                    for (Object object : objects) {
                        ByteBuffer o = (ByteBuffer) object;
                        byte[] array = o.array();
                        arrayList.add(array);
                    }
                    beforeJson.put(beforeField.name(), arrayList);
                    continue;
                }
                beforeJson.put(beforeField.name(), beforeValue);
            }
        }
        // get afterValue
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();

        if (after != null) {
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (Field field : afterFields) {
                Object afterValue = after.get(field);
                String name = field.name();
                if (name.equals("file_ops")){
                    ArrayList afterValue1 = (ArrayList) after.get(field);
                    Object[] objects = afterValue1.toArray();
                    //byte[] b = o.array();
                    ArrayList<byte[]> arrayList = new ArrayList();
                    for (Object object : objects) {
                        ByteBuffer o = (ByteBuffer) object;
                        byte[] array = o.array();
                        arrayList.add(array);
                    }
                    afterJson.put(field.name(), arrayList);
                    continue;
                }
                afterJson.put(field.name(), afterValue);
            }
        }

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        result.put("commitOp",type);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        if (!beforeJson.isEmpty() || !afterJson.isEmpty()) {
            collector.collect(result.toJSONString());
        }
    }


    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}

