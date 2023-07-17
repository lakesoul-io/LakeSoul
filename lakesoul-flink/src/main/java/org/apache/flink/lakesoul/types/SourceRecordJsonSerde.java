// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverter;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.storage.ConverterType;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class SourceRecordJsonSerde implements Serializable {

    private final transient JsonConverter keyJsonConverter;
    private final transient JsonConverter valueJsonConverter;

    public static SourceRecordJsonSerde getInstance(){
        return Inner.instance;
    }
    private static class Inner {
        private static final SourceRecordJsonSerde instance = new SourceRecordJsonSerde();
    }

    private static JsonConverter createConverter(String type) {
        JsonConverter jsonConverter = new JsonConverter();
        HashMap<String, Object> configs = new HashMap<>(2);
        configs.put("converter.type", type);
        configs.put("schemas.enable", true);
        jsonConverter.configure(configs);
        return jsonConverter;
    }

    private SourceRecordJsonSerde() {
        this.keyJsonConverter = createConverter(ConverterType.KEY.getName());
        this.valueJsonConverter = createConverter(ConverterType.VALUE.getName());
    }

    public String serializeKey(SourceRecord record) {
        byte[] bytes = this.keyJsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        return new String(bytes);
    }

    public String serializeValue(SourceRecord record) {
        byte[] bytes = this.valueJsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public SchemaAndValue deserializeKey(String topic, String key) {
        return keyJsonConverter.toConnectData(topic, key.getBytes(StandardCharsets.UTF_8));
    }

    public SchemaAndValue deserializeValue(String topic, String value) {
        return valueJsonConverter.toConnectData(topic, value.getBytes(StandardCharsets.UTF_8));
    }
}
