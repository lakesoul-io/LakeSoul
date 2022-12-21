package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;

import static com.ververica.cdc.connectors.mysql.source.utils.RecordUtils.SCHEMA_CHANGE_EVENT_KEY_NAME;

public class BinarySourceRecord {
    private final String topic;

    private List<String> primaryKeys;

    private final TableId tableId;

    private String tableLocation;

    private List<String> partitionKeys = Collections.emptyList();

    private boolean isDDLRecord;

    LakeSoulRowDataWrapper data;

    public BinarySourceRecord(BinaryRowData rowData, RowType rowType, String topic) {
        this.rowData = rowData;
        this.rowType = rowType;
        this.topic = topic;
        this.tableId = new TableId(io.debezium.relational.TableId.parse(topic).toLowercase());
    }

    public static BinarySourceRecord fromKafkaSourceRecord(SourceRecord sourceRecord) {
        Schema keySchema = sourceRecord.keySchema();
        boolean isDDL = keySchema.name().equalsIgnoreCase(SCHEMA_CHANGE_EVENT_KEY_NAME);
        return null;
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
}
