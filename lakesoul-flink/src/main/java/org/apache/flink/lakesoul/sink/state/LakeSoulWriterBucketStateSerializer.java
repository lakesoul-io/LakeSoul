// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code SimpleVersionedSerializer} used to serialize the {@link LakeSoulWriterBucketState
 * BucketState}.
 */
public class LakeSoulWriterBucketStateSerializer
        implements SimpleVersionedSerializer<LakeSoulWriterBucketState> {

    private static final int MAGIC_NUMBER = 0x1e764b79;

    private final SimpleVersionedSerializer<TableSchemaIdentity> tableSchemaIdentitySerializer;

    private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableSimpleVersionedSerializer;

    public LakeSoulWriterBucketStateSerializer(
            SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                    pendingFileRecoverableSimpleVersionedSerializer) {
        this.pendingFileRecoverableSimpleVersionedSerializer =
                checkNotNull(pendingFileRecoverableSimpleVersionedSerializer);
        this.tableSchemaIdentitySerializer = new TableSchemaIdentitySerializer();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(LakeSoulWriterBucketState state) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serialize(state, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public LakeSoulWriterBucketState deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        validateMagicNumber(in);
        return deserialize(in);
    }

    private void serialize(LakeSoulWriterBucketState state, DataOutputView dataOutputView)
            throws IOException {
        dataOutputView.writeUTF(state.getBucketId());
        dataOutputView.writeUTF(state.getBucketPath().toString());

        SimpleVersionedSerialization.writeVersionAndSerialize(
                tableSchemaIdentitySerializer, state.getIdentity(), dataOutputView);

        dataOutputView.writeInt(state.getPendingFileRecoverableList().size());
        for (int i = 0; i < state.getPendingFileRecoverableList().size(); ++i) {
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    pendingFileRecoverableSimpleVersionedSerializer, state.getPendingFileRecoverableList().get(i),
                    dataOutputView
            );
        }
    }

    private LakeSoulWriterBucketState deserialize(DataInputView in) throws IOException {
        return internalDeserialize(
                in,
                dataInputView ->
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                pendingFileRecoverableSimpleVersionedSerializer, dataInputView));
    }

    private LakeSoulWriterBucketState internalDeserialize(
            DataInputView dataInputView,
            FunctionWithException<DataInputView, InProgressFileWriter.PendingFileRecoverable, IOException>
                    pendingFileDeser)
            throws IOException {

        String bucketId = dataInputView.readUTF();
        String bucketPathStr = dataInputView.readUTF();

        TableSchemaIdentity identity = SimpleVersionedSerialization.readVersionAndDeSerialize(
                tableSchemaIdentitySerializer, dataInputView);

        int pendingFileNum = dataInputView.readInt();
        List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableList = new ArrayList<>();
        for (int i = 0; i < pendingFileNum; ++i) {
            pendingFileRecoverableList.add(pendingFileDeser.apply(dataInputView));
        }

        return new LakeSoulWriterBucketState(
                identity, bucketId,
                new Path(bucketPathStr),
                pendingFileRecoverableList);
    }

    private void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
