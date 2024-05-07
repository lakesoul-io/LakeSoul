package org.apache.flink.lakesoul.sink.datastream;


import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulMultiTableSinkGlobalCommittable;
import org.apache.flink.lakesoul.sink.state.LakeSoulWriterBucketState;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LakeSoulDataStreamSinkBuilder<InputT> implements Sink<InputT, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState,
        LakeSoulMultiTableSinkGlobalCommittable> {


    /**
     * Create a {@link SinkWriter}. If the application is resumed from a checkpoint or savepoint and
     * the sink is stateful, it will receive the corresponding state obtained with {@link
     * SinkWriter#snapshotState(long)} and serialized with {@link #getWriterStateSerializer()}. If
     * no state exists, the first existing, compatible state specified in {@link
     * #getCompatibleStateNames()} will be loaded and passed.
     *
     * @param context the runtime context.
     * @param states  the writer's previous state.
     * @return A sink writer.
     * @throws IOException for any failure during creation.
     * @see SinkWriter#snapshotState(long)
     * @see #getWriterStateSerializer()
     * @see #getCompatibleStateNames()
     */
    @Override
    public SinkWriter<InputT, LakeSoulMultiTableSinkCommittable, LakeSoulWriterBucketState> createWriter(InitContext context, List<LakeSoulWriterBucketState> states) throws IOException {
        return null;
    }

    /**
     * Any stateful sink needs to provide this state serializer and implement {@link
     * SinkWriter#snapshotState(long)} properly. The respective state is used in {@link
     * #createWriter(InitContext, List)} on recovery.
     *
     * @return the serializer of the writer's state type.
     */
    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulWriterBucketState>> getWriterStateSerializer() {
        return Optional.empty();
    }

    /**
     * Creates a {@link Committer} which is part of a 2-phase-commit protocol. The {@link
     * SinkWriter} creates committables through {@link SinkWriter#prepareCommit(boolean)} in the
     * first phase. The committables are then passed to this committer and persisted with {@link
     * Committer#commit(List)}. If a committer is returned, the sink must also return a {@link
     * #getCommittableSerializer()}.
     *
     * @return A committer for the 2-phase-commit protocol.
     * @throws IOException for any failure during creation.
     */
    @Override
    public Optional<Committer<LakeSoulMultiTableSinkCommittable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Creates a {@link GlobalCommitter} which is part of a 2-phase-commit protocol. The {@link
     * SinkWriter} creates committables through {@link SinkWriter#prepareCommit(boolean)} in the
     * first phase. The committables are then passed to the Committer and persisted with {@link
     * Committer#commit(List)}. The committables are also passed to this {@link GlobalCommitter} of
     * which only a single instance exists. If a global committer is returned, the sink must also
     * return a {@link #getCommittableSerializer()} and {@link #getGlobalCommittableSerializer()}.
     *
     * @return A global committer for the 2-phase-commit protocol.
     * @throws IOException for any failure during creation.
     */
    @Override
    public Optional<GlobalCommitter<LakeSoulMultiTableSinkCommittable, LakeSoulMultiTableSinkGlobalCommittable>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    /**
     * Returns the serializer of the committable type. The serializer is required iff the sink has a
     * {@link Committer} or {@link GlobalCommitter}.
     */
    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable>> getCommittableSerializer() {
        return Optional.empty();
    }

    /**
     * Returns the serializer of the aggregated committable type. The serializer is required iff the
     * sink has a {@link GlobalCommitter}.
     */
    @Override
    public Optional<SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }
}
