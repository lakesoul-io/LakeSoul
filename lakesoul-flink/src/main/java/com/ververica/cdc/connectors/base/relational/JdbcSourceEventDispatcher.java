package com.ververica.cdc.connectors.base.relational;

import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkEvent;
import com.ververica.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Schema;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.SchemaBuilder;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Struct;
import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.base.utils.SourceRecordUtils;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.document.DocumentWriter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.spi.SchemaChangeEventEmitter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.schema.DataCollectionFilters;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.HistorizedDatabaseSchema;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcSourceEventDispatcher<P extends Partition> extends EventDispatcher<P, TableId> {
    private static final Logger LOG = LoggerFactory.getLogger(JdbcSourceEventDispatcher.class);
    public static final String HISTORY_RECORD_FIELD = "historyRecord";
    public static final String SERVER_ID_KEY = "server_id";
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    private static final DocumentWriter DOCUMENT_WRITER = DocumentWriter.defaultWriter();
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final HistorizedDatabaseSchema historizedSchema;
    private final DataCollectionFilters.DataCollectionFilter<TableId> filter;
    private final CommonConnectorConfig connectorConfig;
    private final TopicSelector<TableId> topicSelector;
    private final Schema schemaChangeKeySchema;
    private final Schema schemaChangeValueSchema;
    private final String topic;

    public JdbcSourceEventDispatcher(CommonConnectorConfig connectorConfig, TopicSelector<TableId> topicSelector, DatabaseSchema<TableId> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<TableId> filter, ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider, SchemaNameAdjuster schemaNameAdjuster) {
        super(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, metadataProvider, schemaNameAdjuster);
        this.historizedSchema = schema instanceof HistorizedDatabaseSchema ? (HistorizedDatabaseSchema)schema : null;
        this.filter = filter;
        this.queue = queue;
        this.connectorConfig = connectorConfig;
        this.topicSelector = topicSelector;
        this.topic = topicSelector.getPrimaryTopic();
        this.schemaChangeKeySchema = SchemaBuilder.struct().name(schemaNameAdjuster.adjust("io.debezium.connector." + connectorConfig.getConnectorName() + ".SchemaChangeKey")).field("databaseName", Schema.STRING_SCHEMA).build();
        this.schemaChangeValueSchema = SchemaBuilder.struct().name(schemaNameAdjuster.adjust("io.debezium.connector." + connectorConfig.getConnectorName() + ".SchemaChangeValue")).field("source", connectorConfig.getSourceInfoStructMaker().schema()).field("historyRecord", Schema.OPTIONAL_STRING_SCHEMA).build();
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return this.queue;
    }

    public void dispatchSchemaChangeEvent(P partition, TableId dataCollectionId, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        if (dataCollectionId == null || this.filter.isIncluded(dataCollectionId) || this.historizedSchema != null && !this.historizedSchema.storeOnlyCapturedTables()) {
            schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
            IncrementalSnapshotChangeEventSource<P, TableId> incrementalEventSource = this.getIncrementalSnapshotChangeEventSource();
            if (incrementalEventSource != null) {
                incrementalEventSource.processSchemaChange(partition, dataCollectionId);
            }

        } else {
            LOG.trace("Filtering schema change event for {}", dataCollectionId);
        }
    }

    public void dispatchSchemaChangeEvent(Collection<TableId> dataCollectionIds, SchemaChangeEventEmitter schemaChangeEventEmitter) throws InterruptedException {
        boolean anyNonfilteredEvent = false;
        if (dataCollectionIds != null && !dataCollectionIds.isEmpty()) {
            Iterator var4 = dataCollectionIds.iterator();

            while(var4.hasNext()) {
                TableId dataCollectionId = (TableId)var4.next();
                if (this.filter.isIncluded(dataCollectionId)) {
                    anyNonfilteredEvent = true;
                    break;
                }
            }
        } else {
            anyNonfilteredEvent = true;
        }

        if (anyNonfilteredEvent || this.historizedSchema != null && !this.historizedSchema.storeOnlyCapturedTables()) {
            schemaChangeEventEmitter.emitSchemaChangeEvent(new SchemaChangeEventReceiver());
        } else {
            LOG.trace("Filtering schema change event for {}", dataCollectionIds);
        }
    }

    public void dispatchWatermarkEvent(Map<String, ?> sourcePartition, SourceSplitBase sourceSplit, Offset watermark, WatermarkKind watermarkKind) throws InterruptedException {
        SourceRecord sourceRecord = WatermarkEvent.create(sourcePartition, this.topic, sourceSplit.splitId(), watermarkKind, watermark);
        this.queue.enqueue(new DataChangeEvent(sourceRecord));
    }

    private final class SchemaChangeEventReceiver implements SchemaChangeEventEmitter.Receiver {
        private SchemaChangeEventReceiver() {
        }

        private Struct schemaChangeRecordKey(SchemaChangeEvent event) {
            Struct result = new Struct(JdbcSourceEventDispatcher.this.schemaChangeKeySchema);
            result.put("databaseName", event.getDatabase());
            return result;
        }

        private Struct schemaChangeRecordValue(SchemaChangeEvent event) throws IOException {
            Struct sourceInfo = event.getSource();
            Map<String, Object> source = new HashMap();
            if (SourceRecordUtils.isMysqlConnector(sourceInfo)) {
                String fileName = sourceInfo.getString(BINLOG_FILENAME_OFFSET_KEY);
                Long pos = sourceInfo.getInt64(BINLOG_POSITION_OFFSET_KEY);
                Long serverId = sourceInfo.getInt64(SERVER_ID_KEY);
                source.put(SERVER_ID_KEY, serverId);
                source.put(BINLOG_FILENAME_OFFSET_KEY, fileName);
                source.put(BINLOG_POSITION_OFFSET_KEY, pos);
            }
            HistoryRecord historyRecord = new HistoryRecord(source, event.getOffset(), event.getDatabase(), (String)null, event.getDdl(), event.getTableChanges());
            String historyStr = JdbcSourceEventDispatcher.DOCUMENT_WRITER.write(historyRecord.document());
            Struct value = new Struct(JdbcSourceEventDispatcher.this.schemaChangeValueSchema);
            value.put("source", event.getSource());
            value.put("historyRecord", historyStr);
            return value;
        }

        public void schemaChangeEvent(SchemaChangeEvent event) throws InterruptedException {
            JdbcSourceEventDispatcher.this.historizedSchema.applySchemaChange(event);
            if (JdbcSourceEventDispatcher.this.connectorConfig.isSchemaChangesHistoryEnabled()) {
                try {
                    String topicName = JdbcSourceEventDispatcher.this.topicSelector.getPrimaryTopic();
                    Integer partition = 0;
                    Struct key = this.schemaChangeRecordKey(event);
                    Struct value = this.schemaChangeRecordValue(event);
                    SourceRecord record = new SourceRecord(event.getPartition(), event.getOffset(), topicName, partition, JdbcSourceEventDispatcher.this.schemaChangeKeySchema, key, JdbcSourceEventDispatcher.this.schemaChangeValueSchema, value);
                    JdbcSourceEventDispatcher.this.queue.enqueue(new DataChangeEvent(record));
                } catch (IOException var7) {
                    throw new IllegalStateException(String.format("dispatch schema change event %s error ", event), var7);
                }
            }

        }
    }
}