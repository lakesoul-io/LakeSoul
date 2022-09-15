package org.apache.flink.lakeSoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

public final class JsonSourceRecord implements Serializable {
    private final String key;

    private final String value;

    private final String topic;

    public JsonSourceRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public static JsonSourceRecord fromKafkaSourceRecord(SourceRecord sourceRecord, SourceRecordJsonSerde serde) {
        return new JsonSourceRecord(sourceRecord.topic(),
                serde.serializeKey(sourceRecord), serde.serializeValue(sourceRecord));
    }

    public SchemaAndValue getKey(SourceRecordJsonSerde serde) {
        return serde.deserializeKey(topic, key);
    }

    public SchemaAndValue getValue(SourceRecordJsonSerde serde) {
        return serde.deserializeValue(topic, value);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("JsonSourceRecord:\n")
                .append("\tTopic:")
                .append(topic)
                .append("\n\tKey:")
                .append(key)
                .append("\n\tValue:")
                .append(value)
                .toString();
    }
}
