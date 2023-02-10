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

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Field;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaAndValue;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import io.debezium.data.Envelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME;

public class BinarySourceRecord {
    private final String topic;

    private final List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    private final List<String> partitionKeys;

    private final boolean isDDLRecord;

    private final LakeSoulRowDataWrapper data;

    private final String sourceRecordValue;

    public BinarySourceRecord(String topic, List<String> primaryKeys,
                              List<String> partitionKeys,
                              LakeSoulRowDataWrapper data,
                              String sourceRecordValue,
                              TableId tableId,
                              boolean isDDL) {
        this.topic = topic;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.data = data;
        this.sourceRecordValue = sourceRecordValue;
        this.tableId = tableId;
        this.isDDLRecord = isDDL;
    }

    public static BinarySourceRecord fromKafkaSourceRecord(SourceRecord sourceRecord,
                                                           LakeSoulRecordConvert convert) throws Exception {
        Schema keySchema = sourceRecord.keySchema();
        TableId tableId = new TableId(io.debezium.relational.TableId.parse(sourceRecord.topic()).toLowercase());
        boolean isDDL = SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
        if (isDDL) {
            return new BinarySourceRecord(sourceRecord.topic(), null,
                    Collections.emptyList(), null, SourceRecordJsonSerde.getInstance().serializeValue(sourceRecord),
                    tableId,
                    true);
        } else {
            List<String> primaryKeys = new ArrayList<>();
            keySchema.fields().forEach(f -> primaryKeys.add(f.name()));
            Schema valueSchema = sourceRecord.valueSchema();
            Struct value = (Struct) sourceRecord.value();

            // retrieve source event time if exist and non-zero
            Field sourceField = valueSchema.field(Envelope.FieldName.SOURCE);
            long eventTime = 0;
            if (sourceField != null && sourceField.schema().field("ts_ms") != null) {
                Struct source = value.getStruct(Envelope.FieldName.SOURCE);
                if (source != null) {
                    eventTime = (Long) source.getWithoutDefault("ts_ms");
                }
            }
            LakeSoulRowDataWrapper data = convert.toLakeSoulDataType(valueSchema, value, tableId, eventTime);
            return new BinarySourceRecord(sourceRecord.topic(), primaryKeys, Collections.emptyList(), data, null,
                    tableId, false);
        }
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableLocation(String tableLocation) {
        this.tableLocation = tableLocation;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public boolean isDDLRecord() {
        return isDDLRecord;
    }

    public LakeSoulRowDataWrapper getData() {
        return data;
    }

    public SchemaAndValue getDDLStructValue() {
        SourceRecordJsonSerde serde = SourceRecordJsonSerde.getInstance();
        return serde.deserializeValue(topic, sourceRecordValue);
    }

    @Override
    public String toString() {
        return "BinarySourceRecord{" +
                "topic='" + topic + '\'' +
                ", primaryKeys=" + primaryKeys +
                ", tableId=" + tableId +
                ", tableLocation='" + tableLocation + '\'' +
                ", partitionKeys=" + partitionKeys +
                ", isDDLRecord=" + isDDLRecord +
                ", data=" + data +
                ", sourceRecordValue='" + sourceRecordValue + '\'' +
                '}';
    }
}
