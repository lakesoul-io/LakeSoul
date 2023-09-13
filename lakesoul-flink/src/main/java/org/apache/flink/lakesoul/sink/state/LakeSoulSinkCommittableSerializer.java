// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.lakesoul.sink.writer.NativeBucketWriter;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Versioned serializer for {@link LakeSoulMultiTableSinkCommittable}.
 */
public class LakeSoulSinkCommittableSerializer
        implements SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> {

    public static final LakeSoulSinkCommittableSerializer INSTANCE = new LakeSoulSinkCommittableSerializer(NativeBucketWriter.NativePendingFileRecoverableSerializer.INSTANCE);
    private static final int MAGIC_NUMBER = 0x1e765c80;

    private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer;

    private final SimpleVersionedSerializer<TableSchemaIdentity> tableSchemaIdentitySerializer;

    public LakeSoulSinkCommittableSerializer(
            SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                    pendingFileSerializer) {
        this.pendingFileSerializer = checkNotNull(pendingFileSerializer);
        this.tableSchemaIdentitySerializer = new TableSchemaIdentitySerializer();
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(LakeSoulMultiTableSinkCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(committable, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public LakeSoulMultiTableSinkCommittable deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private void serializeV1(LakeSoulMultiTableSinkCommittable committable, DataOutputView dataOutputView)
            throws IOException {

        if (committable.hasPendingFile()) {
            assert committable.getPendingFiles() != null;
            assert committable.getCommitId() != null;

            dataOutputView.writeBoolean(true);
            dataOutputView.writeInt(committable.getPendingFiles().size());
            for (InProgressFileWriter.PendingFileRecoverable pennding :
                    committable.getPendingFiles()) {
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        pendingFileSerializer, pennding, dataOutputView);
            }
            dataOutputView.writeLong(committable.getCreationTime());
            dataOutputView.writeUTF(committable.getCommitId());
            dataOutputView.writeLong(committable.getTsMs());
            dataOutputView.writeUTF(committable.getDmlType());
        } else {
            dataOutputView.writeBoolean(false);
        }

        SimpleVersionedSerialization.writeVersionAndSerialize(
                tableSchemaIdentitySerializer, committable.getIdentity(), dataOutputView);
        dataOutputView.writeUTF(committable.getBucketId());
    }

    private LakeSoulMultiTableSinkCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        List<InProgressFileWriter.PendingFileRecoverable> pendingFile = null;
        String commitId = null;
        long time = Long.MIN_VALUE;
        long dataTsMs = Long.MAX_VALUE;
        String dmlType = null;
        if (dataInputView.readBoolean()) {
            int size = dataInputView.readInt();
            if (size > 0) {
                pendingFile = new ArrayList<>();
                for (int i = 0; i < size; ++i) {
                    pendingFile.add(
                            SimpleVersionedSerialization.readVersionAndDeSerialize(
                                    pendingFileSerializer, dataInputView));
                }
                time = dataInputView.readLong();
                commitId = dataInputView.readUTF();
                dataTsMs = dataInputView.readLong();
                dmlType = dataInputView.readUTF();
            }
        }

        TableSchemaIdentity identity = SimpleVersionedSerialization.readVersionAndDeSerialize(
                tableSchemaIdentitySerializer, dataInputView);
        String bucketId = dataInputView.readUTF();

        return new LakeSoulMultiTableSinkCommittable(
                bucketId, identity, pendingFile, time, commitId, dataTsMs,dmlType);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
