/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class JsonSourceRecord implements Serializable {
    private final String key;

    private final String value;

    private final String topic;

    private List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    private List<String> partitionKeys = Collections.emptyList();

    public JsonSourceRecord(String topic, String key, String value) {
        this.topic = topic;
        this.key = key;
        this.value = value;
        this.tableId = new TableId(io.debezium.relational.TableId.parse(topic).toLowercase());
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

    public String getDatabase() {
        return tableId.schema();
    }

    public String getTableName() {
        return tableId.table();
    }

    public TableId getTableId() {
        return tableId;
    }

    public String getTopic() {
        return topic;
    }

    public JsonSourceRecord fillPrimaryKeys(Schema keySchema) {
        primaryKeys = new ArrayList<>();
        keySchema.fields().forEach(f -> primaryKeys.add(f.name()));
        return this;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append("JsonSourceRecord:\n")
                .append("\tTableId: ")
                .append(tableId.toString())
                .append("\n\tKey: ")
                .append(key)
                .append("\n\tValue: ")
                .append(value);
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            sb.append("\n\tPrimaryKeys: [");
            sb.append(String.join(", ", primaryKeys));
            sb.append("]");
        }
        return sb.toString();
    }
}
