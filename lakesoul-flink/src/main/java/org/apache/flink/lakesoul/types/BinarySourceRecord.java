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
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.fs.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME;

public class BinarySourceRecord {
    public static long MAX_BINLOG_SIZE = 1073741824;

    private final String topic;

    private final List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    private final List<String> partitionKeys;

    private final boolean isDDLRecord;

    private final LakeSoulRowDataWrapper data;

    private final String sourceRecordValue;

    public BinarySourceRecord(String topic, List<String> primaryKeys, TableId tableId, String tableLocation,
                              List<String> partitionKeys, boolean isDDLRecord, LakeSoulRowDataWrapper data,
                              String sourceRecordValue) {
        this.topic = topic;
        this.primaryKeys = primaryKeys;
        this.tableId = tableId;
        this.tableLocation = tableLocation;
        this.partitionKeys = partitionKeys;
        this.isDDLRecord = isDDLRecord;
        this.data = data;
        this.sourceRecordValue = sourceRecordValue;
    }

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
            long binlogFileIndex = 0;
            long binlogPosition = 0;
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceField != null && source != null ) {
                if (sourceField.schema().field("file") != null) {
                    String fileName = (String)source.getWithoutDefault("file");
                    if (StringUtils.isNotBlank(fileName)) {
                        binlogFileIndex = Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1));
                    }
                }
                if (sourceField.schema().field("pos") != null) {
                    binlogPosition = (Long) source.getWithoutDefault("pos");
                }

            }
            LakeSoulRowDataWrapper data = convert.toLakeSoulDataType(valueSchema, value, tableId, 1l);

            return new BinarySourceRecord(sourceRecord.topic(), primaryKeys, Collections.emptyList(), data, null,
                    tableId, false);
        }
    }

    public static BinarySourceRecord fromMysqlSourceRecord(SourceRecord sourceRecord,
                                                           LakeSoulRecordConvert convert,
                                                           String basePath) throws Exception {
        Schema keySchema = sourceRecord.keySchema();
        TableId tableId = new TableId(io.debezium.relational.TableId.parse(sourceRecord.topic()).toLowercase());
        boolean isDDL = SCHEMA_CHANGE_EVENT_KEY_NAME.equalsIgnoreCase(keySchema.name());
        if (isDDL) {
            return null;
        } else {
            List<String> primaryKeys = new ArrayList<>();
            keySchema.fields().forEach(f -> primaryKeys.add(f.name()));
            Schema valueSchema = sourceRecord.valueSchema();
            Struct value = (Struct) sourceRecord.value();

            // retrieve source event time if exist and non-zero
            Field sourceField = valueSchema.field(Envelope.FieldName.SOURCE);
            long binlogFileIndex = 0;
            long binlogPosition = 0;
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceField != null && source != null ) {
                if (sourceField.schema().field("file") != null) {
                    String fileName = (String)source.getWithoutDefault("file");
                    if (StringUtils.isNotBlank(fileName)) {
                        binlogFileIndex = Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1));
                    }
                }
                if (sourceField.schema().field("pos") != null) {
                    binlogPosition = (Long) source.getWithoutDefault("pos");
                }

            }
            long sortField = MAX_BINLOG_SIZE * (2 + binlogFileIndex) + binlogPosition;
            LakeSoulRowDataWrapper data = convert.toLakeSoulDataType(valueSchema, value, tableId, sortField);
            String tablePath = new Path(new Path(basePath, tableId.schema()), tableId.table()).toString();

            return new BinarySourceRecord(sourceRecord.topic(), primaryKeys, tableId, tablePath,
                    Collections.emptyList(), false, data, null);
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
