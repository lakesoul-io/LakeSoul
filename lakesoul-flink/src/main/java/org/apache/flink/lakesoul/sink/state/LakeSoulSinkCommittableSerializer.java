/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Versioned serializer for {@link LakeSoulMultiTableSinkCommittable}. */
public class LakeSoulSinkCommittableSerializer
        implements SimpleVersionedSerializer<LakeSoulMultiTableSinkCommittable> {

    private static final int MAGIC_NUMBER = 0x1e765c80;

    private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
            pendingFileSerializer;

    private final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable>
            inProgressFileSerializer;

    private final SimpleVersionedSerializer<TableSchemaIdentity> tableSchemaIdentitySerializer;

    public LakeSoulSinkCommittableSerializer(
            SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable>
                    pendingFileSerializer,
            SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable>
                    inProgressFileSerializer) {
        this.pendingFileSerializer = checkNotNull(pendingFileSerializer);
        this.inProgressFileSerializer = checkNotNull(inProgressFileSerializer);
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
            assert committable.getFilePaths() != null;
            assert committable.getPendingFiles().size() == committable.getFilePaths().size();
            assert committable.getCommitId() != null;

            dataOutputView.writeBoolean(true);
            dataOutputView.writeInt(committable.getPendingFiles().size());
            for (InProgressFileWriter.PendingFileRecoverable pennding :
                committable.getPendingFiles()) {
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        pendingFileSerializer, pennding, dataOutputView);
            }
            for (String paths : committable.getFilePaths()) {
                dataOutputView.writeUTF(paths);
            }
            dataOutputView.writeLong(committable.getCreationTime());
            dataOutputView.writeUTF(committable.getCommitId());
        } else {
            dataOutputView.writeBoolean(false);
        }

        if (committable.hasInProgressFileToCleanup()) {
            dataOutputView.writeBoolean(true);
            assert committable.getInProgressFileToCleanup() != null;
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    inProgressFileSerializer,
                    committable.getInProgressFileToCleanup(),
                    dataOutputView);
        } else {
            dataOutputView.writeBoolean(false);
        }

        SimpleVersionedSerialization.writeVersionAndSerialize(
                tableSchemaIdentitySerializer, committable.getIdentity(), dataOutputView);
        dataOutputView.writeUTF(committable.getBucketId());
    }

    private LakeSoulMultiTableSinkCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        List<InProgressFileWriter.PendingFileRecoverable> pendingFile = null;
        List<String> filePaths = null;
        String commitId = null;
        long time = Long.MIN_VALUE;
        if (dataInputView.readBoolean()) {
            int size = dataInputView.readInt();
            if (size > 0) {
                pendingFile = new ArrayList<>();
                filePaths = new ArrayList<>();
                for (int i = 0; i < size; ++i) {
                    pendingFile.add(
                            SimpleVersionedSerialization.readVersionAndDeSerialize(
                                    pendingFileSerializer, dataInputView));
                }
                for (int i = 0; i < size; ++i) {
                    filePaths.add(dataInputView.readUTF());
                }
                time = dataInputView.readLong();
                commitId = dataInputView.readUTF();
            }
        }

        InProgressFileWriter.InProgressFileRecoverable inProgressFileToCleanup = null;
        if (dataInputView.readBoolean()) {
            inProgressFileToCleanup =
                    SimpleVersionedSerialization.readVersionAndDeSerialize(
                            inProgressFileSerializer, dataInputView);
        }

        TableSchemaIdentity identity = SimpleVersionedSerialization.readVersionAndDeSerialize(
                tableSchemaIdentitySerializer, dataInputView);
        String bucketId = dataInputView.readUTF();

        return new LakeSoulMultiTableSinkCommittable(
                bucketId, identity, pendingFile, filePaths, time, commitId, inProgressFileToCleanup);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
