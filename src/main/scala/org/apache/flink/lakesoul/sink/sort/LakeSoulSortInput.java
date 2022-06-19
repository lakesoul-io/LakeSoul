package org.apache.flink.lakesoul.sink.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.ExternalSorter;
import org.apache.flink.runtime.operators.sort.PushSorter;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MutableObjectIterator;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class LakeSoulSortInput <T, K> implements StreamTaskInput<T> {

    private final StreamTaskInput<T> wrappedInput;
    private final PushSorter<Tuple2<byte[], StreamRecord<T>>> sorter;
    private final KeySelector<T, K> keySelector;
    private final TypeSerializer<K> keySerializer;
    private final DataOutputSerializer dataOutputSerializer;
    private final ForwardingDataOutput forwardingDataOutput;
    private MutableObjectIterator<Tuple2<byte[], StreamRecord<T>>> sortedInput = null;
    private boolean emittedLast;
    private long watermarkSeen = Long.MIN_VALUE;

    public LakeSoulSortInput(
            StreamTaskInput<T> wrappedInput,
            TypeSerializer<T> typeSerializer,
            TypeSerializer<K> keySerializer,
            KeySelector<T, K> keySelector,
            MemoryManager memoryManager,
            IOManager ioManager,
            boolean objectReuse,
            double managedMemoryFraction,
            Configuration jobConfiguration,
            TaskInvokable containingTask,
            ExecutionConfig executionConfig) {
        try {
            this.forwardingDataOutput = new ForwardingDataOutput();
            this.keySelector = keySelector;
            this.keySerializer = keySerializer;
            int keyLength = keySerializer.getLength();
            final TypeComparator<Tuple2<byte[], StreamRecord<T>>> comparator;
            if (keyLength > 0) {
                this.dataOutputSerializer = new DataOutputSerializer(keyLength);
                comparator = new FixedLengthByteKeyComparator<>(keyLength);
            } else {
                this.dataOutputSerializer = new DataOutputSerializer(64);
                comparator = new VariableLengthByteKeyComparator<>();
            }
            LakeSoulKeyAndValueSerializer<T> keyAndValueSerializer =
                    new LakeSoulKeyAndValueSerializer<>(typeSerializer, keyLength);
            this.wrappedInput = wrappedInput;
            this.sorter =
                    ExternalSorter.newBuilder(
                                    memoryManager,
                                    containingTask,
                                    keyAndValueSerializer,
                                    comparator,
                                    executionConfig)
                            .memoryFraction(managedMemoryFraction)
                            .enableSpilling(
                                    ioManager,
                                    jobConfiguration.get(AlgorithmOptions.SORT_SPILLING_THRESHOLD))
                            .maxNumFileHandles(
                                    jobConfiguration.get(AlgorithmOptions.SPILLING_MAX_FAN))
                            .objectReuse(objectReuse)
                            .largeRecords(
                                    jobConfiguration.get(
                                            AlgorithmOptions.USE_LARGE_RECORDS_HANDLER))
                            .build();
        } catch (MemoryAllocationException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getInputIndex() {
        return wrappedInput.getInputIndex();
    }

    @Override
    public CompletableFuture<Void> prepareSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        throw new UnsupportedOperationException(
                "Checkpoints are not supported with sorted inputs" + " in the BATCH runtime.");
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        try {
            wrappedInput.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        try {
            sorter.close();
        } catch (IOException e) {
            ex = ExceptionUtils.firstOrSuppressed(e, ex);
        }

        if (ex != null) {
            throw ex;
        }
    }

    private class ForwardingDataOutput implements DataOutput<T> {
        @Override
        public void emitRecord(StreamRecord<T> streamRecord) throws Exception {
            K key = keySelector.getKey(streamRecord.getValue());

            keySerializer.serialize(key, dataOutputSerializer);
            byte[] serializedKey = dataOutputSerializer.getCopyOfBuffer();
            dataOutputSerializer.clear();

            sorter.writeRecord(Tuple2.of(serializedKey, streamRecord));
        }

        @Override
        public void emitWatermark(Watermark watermark) {
            watermarkSeen = Math.max(watermarkSeen, watermark.getTimestamp());
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}
    }

    @Override
    public DataInputStatus emitNext(DataOutput<T> output) throws Exception {
        if (sortedInput != null) {
            return emitNextSortedRecord(output);
        }

        DataInputStatus inputStatus = wrappedInput.emitNext(forwardingDataOutput);
        if (inputStatus == DataInputStatus.END_OF_DATA) {
            endSorting();
            return emitNextSortedRecord(output);
        }

        return inputStatus;
    }

    @Nonnull
    private DataInputStatus emitNextSortedRecord(DataOutput<T> output) throws Exception {
        if (emittedLast) {
            return DataInputStatus.END_OF_INPUT;
        }

        Tuple2<byte[], StreamRecord<T>> next = sortedInput.next();
        if (next != null) {
            output.emitRecord(next.f1);
            return DataInputStatus.MORE_AVAILABLE;
        } else {
            emittedLast = true;
            if (watermarkSeen > Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(watermarkSeen));
            }
            return DataInputStatus.END_OF_DATA;
        }
    }

    private void endSorting() throws Exception {
        this.sorter.finishReading();
        this.sortedInput = sorter.getIterator();
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (sortedInput != null) {
            return AvailabilityProvider.AVAILABLE;
        } else {
            return wrappedInput.getAvailableFuture();
        }
    }
}
