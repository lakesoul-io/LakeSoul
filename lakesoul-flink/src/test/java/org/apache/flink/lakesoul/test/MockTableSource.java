package org.apache.flink.lakesoul.test;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Supplier;

import static org.apache.flink.lakesoul.test.fail.LakeSoulSinkFailTest.generateObjectWithIndexByDatatype;

public class MockTableSource implements ScanTableSource {

    private static final Logger LOG = LoggerFactory.getLogger(MockTableSource.class);

    public enum StopBehavior {
        NO_FAILURE,
        FAIL_ON_BEFORE_ASSIGN_SPLIT,
        FAIL_ON_ASSIGN_SPLIT_FINISHED,
        FAIL_ON_CHECKPOINTING,
        FAIL_ON_COLLECT_FINISHED,
        STOP_POSTGRES_ON_CHECKPOINTING,
    }

    public static Optional<Tuple2<Integer, Integer>> FAIL_OPTION = Optional.empty();
    public static long FAIL_OVER_INTERVAL_START = 0L;
    public static long FAIL_OVER_INTERVAL_END = 0L;

    public static void tryStop(StopBehavior targetBehavior, StopBehavior behavior) {
        if (targetBehavior != behavior) return;

        long current = System.currentTimeMillis();
        if (current > FAIL_OVER_INTERVAL_START && current < FAIL_OVER_INTERVAL_END) {
            String msg = "Sink fail with " + behavior + " at " + LocalDateTime.now();
            LOG.warn(msg);
            if (behavior == StopBehavior.STOP_POSTGRES_ON_CHECKPOINTING) {
                PostgresContainerHelper.stopPostgresForMills(8 * 1000);
                return;
            }

            throw new RuntimeException(msg);
        }
    }

    private final DataType dataType;
    private final String name;
    private final Integer parallelism;
    private final StopBehavior stopBehavior;

    public MockTableSource(DataType dataType, String name, Integer parallelism, StopBehavior stopBehavior) {
        this.dataType = dataType;
        this.name = name;
        this.parallelism = parallelism;
        this.stopBehavior = stopBehavior;
    }


    /**
     * Creates a copy of this instance during planning. The copy should be a deep copy of all
     * mutable members.
     */
    @Override
    public DynamicTableSource copy() {
        return new MockTableSource(dataType, name, parallelism, stopBehavior);
    }

    /**
     * Returns a string that summarizes this source for printing to a console or log.
     */
    @Override
    public String asSummaryString() {
        return null;
    }

    /**
     * Returns the set of changes that the planner can expect during runtime.
     *
     * @see RowKind
     */
    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    /**
     * Returns a provider of runtime implementation for reading the data.
     *
     * <p>There might exist different interfaces for runtime implementation which is why {@link
     * ScanRuntimeProvider} serves as the base interface. Concrete {@link ScanRuntimeProvider}
     * interfaces might be located in other Flink modules.
     *
     * <p>Independent of the provider interface, the table runtime expects that a source
     * implementation emits internal data structures (see {@link
     * RowData} for more information).
     *
     * <p>The given {@link ScanContext} offers utilities by the planner for creating runtime
     * implementation with minimal dependencies to internal data structures.
     *
     * <p>{@link SourceProvider} is the recommended core interface. {@code SourceFunctionProvider}
     * in {@code flink-table-api-java-bridge} and {@link InputFormatProvider} are available for
     * backwards compatibility.
     *
     * @param runtimeProviderContext
     * @see SourceProvider
     */
    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceProvider.of(new ExactlyOnceSource(dataType, stopBehavior));
    }

    static class ExactlyOnceSource implements Source<RowData, MockSplit, Integer> {

        private final DataType dataType;
        private final StopBehavior stopBehavior;

        public ExactlyOnceSource(DataType dataType, StopBehavior stopBehavior) {
            this.dataType = dataType;
            this.stopBehavior = stopBehavior;
        }

        /**
         * Get the boundedness of this source.
         *
         * @return the boundedness of this source.
         */
        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        /**
         * Creates a new reader to read data from the splits it gets assigned. The reader starts fresh
         * and does not have any state to resume.
         *
         * @param readerContext The {@link SourceReaderContext context} for the source reader.
         * @return A new SourceReader.
         * @throws Exception The implementor is free to forward all exceptions directly. Exceptions
         *                   thrown from this method cause task failure/recovery.
         */
        @Override
        public SourceReader<RowData, MockSplit> createReader(SourceReaderContext readerContext) throws Exception {
            return new MockSourceReader(() -> new MockSplitReader(dataType, stopBehavior), (rowData, sourceOutput, split) -> {
                sourceOutput.collect(rowData);
                tryStop(StopBehavior.FAIL_ON_COLLECT_FINISHED, stopBehavior);
            }, readerContext.getConfiguration(), readerContext);
        }

        /**
         * Creates a new SplitEnumerator for this source, starting a new input.
         *
         * @param enumContext The {@link SplitEnumeratorContext context} for the split enumerator.
         * @return A new SplitEnumerator.
         * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
         *                   thrown from this method cause JobManager failure/recovery.
         */
        @Override
        public SplitEnumerator<MockSplit, Integer> createEnumerator(SplitEnumeratorContext<MockSplit> enumContext) throws Exception {
            return new MockSplitEnumerator(enumContext, stopBehavior);
        }

        /**
         * Restores an enumerator from a checkpoint.
         *
         * @param enumContext The {@link SplitEnumeratorContext context} for the restored split
         *                    enumerator.
         * @param checkpoint  The checkpoint to restore the SplitEnumerator from.
         * @return A SplitEnumerator restored from the given checkpoint.
         * @throws Exception The implementor is free to forward all exceptions directly. * Exceptions
         *                   thrown from this method cause JobManager failure/recovery.
         */
        @Override
        public SplitEnumerator<MockSplit, Integer> restoreEnumerator(SplitEnumeratorContext<MockSplit> enumContext, Integer checkpoint) throws Exception {
            return new MockSplitEnumerator(enumContext, stopBehavior, checkpoint);
        }

        /**
         * Creates a serializer for the source splits. Splits are serialized when sending them from
         * enumerator to reader, and when checkpointing the reader's current state.
         *
         * @return The serializer for the split type.
         */
        @Override
        public SimpleVersionedSerializer<MockSplit> getSplitSerializer() {
            return new SimpleVersionedSerializer<MockSplit>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                @Override
                public byte[] serialize(MockSplit split) throws IOException {
                    DataOutputSerializer out = new DataOutputSerializer(2);
                    out.writeInt(split.getIndex());
                    return out.getCopyOfBuffer();
                }

                @Override
                public MockSplit deserialize(int version, byte[] serialized) throws IOException {
                    DataInputDeserializer in = new DataInputDeserializer(serialized);
                    if (version == 0) {
                        return new MockSplit(in.readInt());
                    }
                    throw new IOException("Unrecognized version or corrupt state: " + version);
                }
            };
        }

        /**
         * Creates the serializer for the {@link SplitEnumerator} checkpoint. The serializer is used for
         * the result of the {@link SplitEnumerator #snapshotState()} method.
         *
         * @return The serializer for the SplitEnumerator checkpoint.
         */
        @Override
        public SimpleVersionedSerializer<Integer> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<Integer>() {
                @Override
                public int getVersion() {
                    return 0;
                }

                /**
                 * Serializes the given object. The serialization is assumed to correspond to the current
                 * serialization version (as returned by {@link #getVersion()}.
                 *
                 * @param obj The object to serialize.
                 * @return The serialized data (bytes).
                 * @throws IOException Thrown, if the serialization fails.
                 */
                @Override
                public byte[] serialize(Integer obj) throws IOException {
                    DataOutputSerializer out = new DataOutputSerializer(2);
                    out.writeInt(obj);
                    return out.getCopyOfBuffer();
                }


                @Override
                public Integer deserialize(int version, byte[] serialized) throws IOException {
                    DataInputDeserializer in = new DataInputDeserializer(serialized);
                    if (version == 0) {
                        return in.readInt();
                    }
                    throw new IOException("Unrecognized version or corrupt state: " + version);
                }
            };
        }


    }

    static class MockSplit implements SourceSplit, Serializable {

        private final int index;

        public int getIndex() {
            return index;
        }

        MockSplit(int index) {
            this.index = index;
        }

        /**
         * Get the split id of this source split.
         *
         * @return id of this source split.
         */
        @Override
        public String splitId() {
            return String.valueOf(index);
        }

        @Override
        public String toString() {
            return "MockSplit{" +
                    "index=" + index +
                    '}';
        }
    }

    public static class MockSplitEnumerator implements SplitEnumerator<MockSplit, Integer> {
        private final StopBehavior stopBehavior;
        private int index;

        private final SplitEnumeratorContext<MockSplit> enumContext;
        public static int indexBound = 20;
        private final ArrayDeque<MockSplit> backSplits;

        public MockSplitEnumerator(SplitEnumeratorContext<MockSplit> enumContext, StopBehavior behavior) {
            this(enumContext, behavior, 0);
        }

        public MockSplitEnumerator(SplitEnumeratorContext<MockSplit> enumContext, StopBehavior behavior, int index) {
            this.enumContext = enumContext;
            this.index = index;
            this.stopBehavior = behavior;
            backSplits = new ArrayDeque<>();
        }

        @Override
        public void start() {
            if (FAIL_OPTION.isPresent()) {
                FAIL_OVER_INTERVAL_START = System.currentTimeMillis() + FAIL_OPTION.get().f0;
                FAIL_OVER_INTERVAL_END = FAIL_OVER_INTERVAL_START + FAIL_OPTION.get().f1;
                FAIL_OPTION = Optional.empty();
                String msg = "Sink will fail with " + stopBehavior + " from " + LocalDateTime.ofInstant(Instant.ofEpochMilli(FAIL_OVER_INTERVAL_START), ZoneId.systemDefault()) + " to " +
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(FAIL_OVER_INTERVAL_END), ZoneId.systemDefault());
                LOG.warn(msg);
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {
            LOG.warn("notifyCheckpointAborted: " + checkpointId);
            SplitEnumerator.super.notifyCheckpointAborted(checkpointId);
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            LOG.warn("notifyCheckpointComplete: " + checkpointId);
            SplitEnumerator.super.notifyCheckpointComplete(checkpointId);
        }

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
            tryStop(StopBehavior.FAIL_ON_BEFORE_ASSIGN_SPLIT, stopBehavior);
            if (backSplits.isEmpty()) {
                if (index < indexBound) {
                    LOG.warn("assignSplit: " + index);
                    enumContext.assignSplit(new MockSplit(index), subtaskId);
                    index++;
                }
            } else {
                enumContext.assignSplit(backSplits.pop(), subtaskId);
            }
            tryStop(StopBehavior.FAIL_ON_ASSIGN_SPLIT_FINISHED, stopBehavior);
        }

        @Override
        public void addSplitsBack(List<MockSplit> splits, int subtaskId) {
            backSplits.addAll(splits);
        }

        @Override
        public void addReader(int subtaskId) {

        }

        @Override
        public Integer snapshotState(long checkpointId) throws Exception {
            tryStop(StopBehavior.FAIL_ON_CHECKPOINTING, stopBehavior);
            tryStop(StopBehavior.STOP_POSTGRES_ON_CHECKPOINTING, stopBehavior);
            return index;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class MockSourceReader extends SingleThreadMultiplexSourceReaderBase<RowData, RowData, MockSplit, MockSplit> {

        public MockSourceReader(Supplier<SplitReader<RowData, MockSplit>> splitReaderSupplier, RecordEmitter<RowData, RowData, MockSplit> recordEmitter, Configuration config, SourceReaderContext context) {
            super(splitReaderSupplier, recordEmitter, config, context);
        }

        @Override
        public void start() {
            if (getNumberOfCurrentlyAssignedSplits() == 0) {
                context.sendSplitRequest();
            }
        }

        @Override
        protected void onSplitFinished(Map<String, MockSplit> map) {
            try {
                int tps = 3;
                Thread.sleep(1000 / tps);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            context.sendSplitRequest();
        }

        @Override
        protected MockSplit initializedState(MockSplit split) {
            return split;
        }

        @Override
        protected MockSplit toSplitType(String s, MockSplit split) {
            return split;
        }
    }

    static class MockSplitReader implements SplitReader<RowData, MockSplit> {
        private final ArrayDeque<MockSplit> splits;

        private final DataType dataType;
        private final StopBehavior stopBehavior;

        public MockSplitReader(DataType dataType, StopBehavior stopBehavior) {
            this.dataType = dataType;
            this.stopBehavior = stopBehavior;
            splits = new ArrayDeque<>();
        }

        @Override
        public RecordsWithSplitIds<RowData> fetch() {
            return new MockRecordsWithSplitIds(splits.pop(), dataType, stopBehavior);
        }

        @Override
        public void handleSplitsChanges(SplitsChange<MockSplit> splitsChange) {
            if (!(splitsChange instanceof SplitsAddition)) {
                throw new UnsupportedOperationException(
                        String.format("The SplitChange type of %s is not supported.", splitsChange.getClass()));
            }

            splits.addAll(splitsChange.splits());
        }

        @Override
        public void wakeUp() {

        }

        @Override
        public void close() throws Exception {

        }
    }

    static class MockRecordsWithSplitIds implements RecordsWithSplitIds<RowData> {

        final MockSplit split;

        boolean finished;

        private final RowType rowType;
        private final StopBehavior stopBehavior;

        MockRecordsWithSplitIds(MockSplit mockSplit, DataType dataType, StopBehavior stopBehavior) {
            this.rowType = (RowType) dataType.getLogicalType();
            this.stopBehavior = stopBehavior;
            split = mockSplit;
            finished = false;
        }

        /**
         * Moves to the next split. This method is also called initially to move to the first split.
         * Returns null, if no splits are left.
         */
        @Nullable
        @Override
        public String nextSplit() {
            return finished ? null : split.splitId();
        }

        /**
         * Gets the next record from the current split. Returns null if no more records are left in this
         * split.
         */
        @Nullable
        @Override
        public RowData nextRecordFromSplit() {
            RowData next = finished ? null : GenericRowData.of(rowType.getFields().stream().map(field -> generateObjectWithIndexByDatatype(split.getIndex(), field)).toArray());
            finished = true;
            return next;
        }

        /**
         * Get the finished splits.
         *
         * @return the finished splits after this RecordsWithSplitIds is returned.
         */
        @Override
        public Set<String> finishedSplits() {
            return Collections.singleton(split.splitId());
        }
    }
}
