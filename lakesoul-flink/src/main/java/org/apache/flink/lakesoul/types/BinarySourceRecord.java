// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.*;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import io.debezium.data.Envelope;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;

import java.util.*;

public class BinarySourceRecord {

    private final String topic;

    private final List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    public  List<String> partitionKeys ;

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
                              boolean isDDL)
    {

        this.topic = topic;
        this.primaryKeys = primaryKeys;
        this.partitionKeys = partitionKeys;
        this.data = data;
        this.sourceRecordValue = sourceRecordValue;
        this.tableId = tableId;
        this.isDDLRecord = isDDL;
    }

    public static BinarySourceRecord fromMysqlSourceRecord(SourceRecord sourceRecord,
                                                           LakeSoulRecordConvert convert,
                                                           String basePath,
                                                           String sinkDBName) throws Exception {
        Schema keySchema = sourceRecord.keySchema();
        TableId tableId = new TableId(io.debezium.relational.TableId.parse(sourceRecord.topic()).toLowercase());
        String sourceSchemaName = tableId.schema() == null ? tableId.catalog() : tableId.schema();
        String tableName;
        String originTableName;
        if (sinkDBName.equals(sourceSchemaName) || sourceRecord.topic().split("\\.")[0].equals("mysql_binlog_source")){
            tableName = String.format("s_%s_%s", sourceSchemaName, tableId.table()).toLowerCase();
            originTableName = tableId.table();
            tableId = new TableId(sinkDBName, sourceSchemaName , tableName);
        } else {
            tableName = String.format("s_%s_%s_%s", sinkDBName, sourceSchemaName, tableId.table()).toLowerCase();
            originTableName = tableId.table();
            tableId = new TableId(sinkDBName, sourceSchemaName , tableName);
        }
        HashMap<String, List<String>> topicsPartitionFields = convert.topicsPartitionFields;
        if (topicsPartitionFields.containsKey(originTableName)) {
            List<String> partitionColls = topicsPartitionFields.get(originTableName);
            topicsPartitionFields.remove(originTableName);
            topicsPartitionFields.put(tableName, partitionColls);
            if (convert.formatRuleList.containsKey(originTableName)){
                HashMap<String, String> formatRuleList = convert.formatRuleList;
                String fomatRule = formatRuleList.get(originTableName);
                formatRuleList.remove(originTableName);
                formatRuleList.put(tableName, fomatRule);
            }
        }

        boolean isDDL = "io.debezium.connector.mysql.SchemaChangeKey".equalsIgnoreCase(keySchema.name());
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
            long tsMs = 0;
            Struct source = value.getStruct(Envelope.FieldName.SOURCE);
            if (sourceField != null && source != null) {
                if (sourceField.schema().field("file") != null) {
                    String fileName = (String) source.getWithoutDefault("file");
                    if (StringUtils.isNotBlank(fileName)) {
                        binlogFileIndex = Long.parseLong(fileName.substring(fileName.lastIndexOf(".") + 1));
                    }
                }
                if (sourceField.schema().field("pos") != null) {
                    binlogPosition = (Long) source.getWithoutDefault("pos");
                }
                if (sourceField.schema().field("ts_ms") != null) {
                    tsMs = (Long) source.getWithoutDefault("ts_ms");
                    if (tsMs == 0) {
                        tsMs = System.currentTimeMillis();
                    }
                }
            }
            long sortField = (binlogFileIndex << 32) + binlogPosition;
            LakeSoulRowDataWrapper data = convert.toLakeSoulDataType(valueSchema, value, tableId, tsMs, sortField);
            String tablePath;
            if (tableId.schema() == null) {
                tablePath = new Path(new Path(basePath, tableId.catalog()), tableId.table()).toString();
            } else {
                tablePath = new Path(new Path(basePath, tableId.schema()), tableId.table()).toString();
            }
            List<String> newPartitionFields = convert.topicsPartitionFields.get(tableId.table());
            return new BinarySourceRecord(sourceRecord.topic(), primaryKeys, tableId,
                    FlinkUtil.makeQualifiedPath(tablePath).toString(),
                    newPartitionFields == null ? Collections.emptyList(): newPartitionFields, false, data, null);
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
    public static SourceRecord addEventDate(SourceRecord record, String eventDate) {

        Schema oldSchema = record.valueSchema();
        Struct oldValue = (Struct) record.value();
        SchemaBuilder newSchemaBuilder = SchemaBuilder.struct()
                .name(oldSchema.name())
                .optional();

        oldSchema.fields().forEach(field ->
                newSchemaBuilder.field(field.name(), field.schema())
        );
        newSchemaBuilder.field("eventDate", Schema.STRING_SCHEMA);
        Schema newSchema = newSchemaBuilder.build();
        Struct newValue = new Struct(newSchema);
        oldSchema.fields().forEach(field ->
                newValue.put(field.name(), oldValue.get(field))
        );

        newValue.put("eventDate", eventDate);
        return new SourceRecord(
                record.sourcePartition(),
                record.sourceOffset(),
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp()
        );
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