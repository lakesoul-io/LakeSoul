package org.apache.flink.lakeSoul.sink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakeSoul.metaData.DataFileMetaData;
import org.apache.flink.lakeSoul.sink.writer.LakesSoulOneTableWriter;
import org.apache.flink.lakeSoul.sink.writer.TableSchemaWriterCreator;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

public class LakeSoulMultiTableSink extends AbstractStreamOperator<DataFileMetaData>
        implements OneInputStreamOperator<JsonSourceRecord, DataFileMetaData>, BoundedOneInput {

    private static final MapStateDescriptor<TableSchemaWriterCreator.TableSchemaIdentity, List<byte[]>> BUCKET_STATE_DESC =
            new MapStateDescriptor<>("multi-bucket-states",
                    TypeInformation.of(new TypeHint<>() {}),
                    new ListTypeInfo<>(BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
    private static final MapStateDescriptor<TableSchemaWriterCreator.TableSchemaIdentity, List<Long>> MAX_PART_COUNTER_STATE_DESC =
            new MapStateDescriptor<>("multi-max-part-counter",
                    TypeInformation.of(new TypeHint<>() {}),
                    new ListTypeInfo<>(LONG_TYPE_INFO));
    private static final MapStateDescriptor<TableSchemaWriterCreator.TableSchemaIdentity, TableSchemaWriterCreator> TABLE_CREATOR_STATE_DESC =
            new MapStateDescriptor<>("multi-table-creator",
                    TypeInformation.of(new TypeHint<>() {}),
                    TypeInformation.of(new TypeHint<>() {}));

    private final ConcurrentHashMap<TableSchemaWriterCreator.TableSchemaIdentity, LakesSoulOneTableWriter<RowData>> writers;

    private final ConcurrentHashMap<TableSchemaWriterCreator.TableSchemaIdentity, TableSchemaWriterCreator> writerCreators;

    private final OutputFileConfig outputFileConfig;
    private final Configuration conf;

    int bucketCheckInterval;

    public LakeSoulMultiTableSink(Configuration conf, OutputFileConfig outputFileConfig) {
        this.writers = new ConcurrentHashMap<>();
        this.writerCreators = new ConcurrentHashMap<>();
        this.conf = conf;
        this.outputFileConfig = outputFileConfig;
    }

    @Override
    public void endInput() {
        writers.forEach((k, v) -> {
            try {
                v.endInput(output);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void processElement(StreamRecord<JsonSourceRecord> element) throws Exception {
        // TODO: retrieve rowdata and row type from source record
        TableSchemaWriterCreator.TableSchemaIdentity identity = new TableSchemaWriterCreator.TableSchemaIdentity(
                element.getValue().getTableId(), null);
        LakesSoulOneTableWriter<RowData> writer =
                writers.computeIfAbsent(identity, key0 -> {
                    LakesSoulOneTableWriter<RowData> w = writerCreators.computeIfAbsent(key0, key1 -> {
                        try {
                            return TableSchemaWriterCreator.create(
                                    element.getValue().getTableId(), null,
                                    null, element.getValue().getPrimaryKeys(),
                                    List.of(),
                                    conf);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }).createTableSchemaWriter(bucketCheckInterval, getUserCodeClassloader(), conf, getRuntimeContext());
                    try {
                        w.initializeState(false, null, null);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return w;
                });

        writer.processElement(new StreamRecord<>(null));
    }

    @Override
    public void setKeyContextElement(StreamRecord<JsonSourceRecord> record) throws Exception {
        OneInputStreamOperator.super.setKeyContextElement(record);
    }

    @Override
    public void finish() {
        writers.forEach((k, v) -> v.finish());
    }

    @Override
    public void close() throws Exception {
        writers.forEach((k, v) -> v.close());
        super.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        for (Map.Entry<TableSchemaWriterCreator.TableSchemaIdentity,
                       LakesSoulOneTableWriter<RowData>> entry : writers.entrySet()) {
            LakesSoulOneTableWriter<RowData> writer = entry.getValue();
            writer.commitUpToCheckpoint(checkpointId);
            NavigableMap<Long, Set<String>> headBuckets = writer.getNewBuckets().headMap(checkpointId, true);
            Set<String> partitions = new HashSet<>(writer.getCommittableBuckets());
            writer.getCommittableBuckets().clear();
            if (partitions.isEmpty()) {
                return;
            }
            headBuckets.values().forEach(partitions::addAll);
            headBuckets.clear();
            String pathPre = outputFileConfig.getPartPrefix() + "-";
            DataFileMetaData newFileMeta = new DataFileMetaData(checkpointId,
                    getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks(),
                    new ArrayList<>(partitions), pathPre, entry.getKey().tableId.table());
            output.collect(new StreamRecord<>(newFileMeta));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        MapState<TableSchemaWriterCreator.TableSchemaIdentity, List<byte[]>> bucketStates =
                getRuntimeContext().getMapState(BUCKET_STATE_DESC);
        MapState<TableSchemaWriterCreator.TableSchemaIdentity, List<Long>> counterStates =
                getRuntimeContext().getMapState(MAX_PART_COUNTER_STATE_DESC);
        MapState<TableSchemaWriterCreator.TableSchemaIdentity, TableSchemaWriterCreator> creatorStates =
                getRuntimeContext().getMapState(TABLE_CREATOR_STATE_DESC);
        creatorStates.putAll(writerCreators);
        bucketStates.clear();
        counterStates.clear();
        for (Map.Entry<TableSchemaWriterCreator.TableSchemaIdentity,
                       LakesSoulOneTableWriter<RowData>> entry : writers.entrySet()) {
            LakesSoulOneTableWriter<RowData> writer = entry.getValue();
            writer.snapshotState(context);
            bucketStates.put(entry.getKey(), writer.getBucketStates());
            counterStates.put(entry.getKey(), writer.getMaxPartCountersStates());
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        if (context.isRestored()) {
            MapState<TableSchemaWriterCreator.TableSchemaIdentity, List<byte[]>> bucketStates =
                    getRuntimeContext().getMapState(BUCKET_STATE_DESC);
            MapState<TableSchemaWriterCreator.TableSchemaIdentity, List<Long>> counterStates =
                    getRuntimeContext().getMapState(MAX_PART_COUNTER_STATE_DESC);
            MapState<TableSchemaWriterCreator.TableSchemaIdentity, TableSchemaWriterCreator> creatorStates =
                    getRuntimeContext().getMapState(TABLE_CREATOR_STATE_DESC);

            for (Map.Entry<TableSchemaWriterCreator.TableSchemaIdentity,
                    TableSchemaWriterCreator> entry : creatorStates.entries()) {
                TableSchemaWriterCreator.TableSchemaIdentity key = entry.getKey();
                TableSchemaWriterCreator creator = writerCreators.put(key, entry.getValue());
                if (creator != null && !writers.contains(key)) {
                    LakesSoulOneTableWriter<RowData> writer =
                            creator.createTableSchemaWriter(
                                    bucketCheckInterval, getUserCodeClassloader(), conf, getRuntimeContext());
                    writers.put(key, writer);
                    // initialize state for writer
                    if (bucketStates.contains(entry.getKey()) && counterStates.contains(entry.getKey())) {
                        writer.initializeState(true, bucketStates.get(key), counterStates.get(key));
                    } else {
                        writer.initializeState(false, null, null);
                    }
                }
            }
        }
    }
}
