/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.sink.fileSystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class BucketStateSerializer<BucketID> implements SimpleVersionedSerializer<BucketState<BucketID>> {
  private static final int MAGIC_NUMBER = 511069049;
  private final SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer;
  private final SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableSerializer;
  private final SimpleVersionedSerializer<BucketID> bucketIdSerializer;

  BucketStateSerializer(SimpleVersionedSerializer<InProgressFileWriter.InProgressFileRecoverable> inProgressFileRecoverableSerializer,
                        SimpleVersionedSerializer<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableSerializer, SimpleVersionedSerializer<BucketID> bucketIdSerializer) {
    this.inProgressFileRecoverableSerializer = (SimpleVersionedSerializer) Preconditions.checkNotNull(inProgressFileRecoverableSerializer);
    this.pendingFileRecoverableSerializer = (SimpleVersionedSerializer) Preconditions.checkNotNull(pendingFileRecoverableSerializer);
    this.bucketIdSerializer = (SimpleVersionedSerializer) Preconditions.checkNotNull(bucketIdSerializer);
  }

  @Override
  public int getVersion() {
    return 2;
  }

  @Override
  public byte[] serialize(BucketState<BucketID> state) throws IOException {
    DataOutputSerializer out = new DataOutputSerializer(256);
    out.writeInt(511069049);
    this.serializeV2(state, out);
    return out.getCopyOfBuffer();
  }

  @Override
  public BucketState<BucketID> deserialize(int version, byte[] serialized) throws IOException {
    DataInputDeserializer in = new DataInputDeserializer(serialized);
    switch (version) {
      case 1:
        validateMagicNumber(in);
        return this.deserializeV1(in);
      case 2:
        validateMagicNumber(in);
        return this.deserializeV2(in);
      default:
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }
  }

  private void serializeV2(BucketState<BucketID> state, DataOutputView dataOutputView) throws IOException {
    SimpleVersionedSerialization.writeVersionAndSerialize(this.bucketIdSerializer, state.getBucketId(), dataOutputView);
    dataOutputView.writeUTF(state.getBucketPath().toString());
    dataOutputView.writeLong(state.getInProgressFileCreationTime());
    if (state.hasInProgressFileRecoverable()) {
      InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable = state.getInProgressFileRecoverable();
      dataOutputView.writeBoolean(true);
      SimpleVersionedSerialization.writeVersionAndSerialize(this.inProgressFileRecoverableSerializer, inProgressFileRecoverable, dataOutputView);
    } else {
      dataOutputView.writeBoolean(false);
    }

    Map<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverables = state.getPendingFileRecoverablesPerCheckpoint();
    dataOutputView.writeInt(this.pendingFileRecoverableSerializer.getVersion());
    dataOutputView.writeInt(pendingFileRecoverables.size());
    Iterator var4 = pendingFileRecoverables.entrySet().iterator();

    while (var4.hasNext()) {
      Map.Entry<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFilesForCheckpoint = (Map.Entry) var4.next();
      List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverableList = (List) pendingFilesForCheckpoint.getValue();
      dataOutputView.writeLong((Long) pendingFilesForCheckpoint.getKey());
      dataOutputView.writeInt(pendingFileRecoverableList.size());
      Iterator var7 = pendingFileRecoverableList.iterator();

      while (var7.hasNext()) {
        InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable = (InProgressFileWriter.PendingFileRecoverable) var7.next();
        byte[] serialized = this.pendingFileRecoverableSerializer.serialize(pendingFileRecoverable);
        dataOutputView.writeInt(serialized.length);
        dataOutputView.write(serialized);
      }
    }

  }

  private BucketState<BucketID> deserializeV1(DataInputView in) throws IOException {
    SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer = this.getCommitableSerializer();
    SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer = this.getResumableSerializer();
    BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(this.bucketIdSerializer, in);
    String bucketPathStr = in.readUTF();
    long creationTime = in.readLong();
    InProgressFileWriter.InProgressFileRecoverable current = null;
    if (in.readBoolean()) {
      current = new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverable(
          (RecoverableWriter.ResumeRecoverable) SimpleVersionedSerialization.readVersionAndDeSerialize(resumableSerializer, in));
    }

    int committableVersion = in.readInt();
    int numCheckpoints = in.readInt();
    HashMap<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablePerCheckpoint = new HashMap(numCheckpoints);

    for (int i = 0; i < numCheckpoints; ++i) {
      long checkpointId = in.readLong();
      int noOfResumables = in.readInt();
      List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables = new ArrayList(noOfResumables);

      for (int j = 0; j < noOfResumables; ++j) {
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        pendingFileRecoverables.add(
            new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable((RecoverableWriter.CommitRecoverable) commitableSerializer.deserialize(committableVersion, bytes)));
      }

      pendingFileRecoverablePerCheckpoint.put(checkpointId, pendingFileRecoverables);
    }

    return new BucketState(bucketId, new Path(bucketPathStr), creationTime, current, pendingFileRecoverablePerCheckpoint);
  }

  private BucketState<BucketID> deserializeV2(DataInputView dataInputView) throws IOException {
    BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(this.bucketIdSerializer, dataInputView);
    String bucketPathStr = dataInputView.readUTF();
    long creationTime = dataInputView.readLong();
    InProgressFileWriter.InProgressFileRecoverable current = null;
    if (dataInputView.readBoolean()) {
      current = (InProgressFileWriter.InProgressFileRecoverable) SimpleVersionedSerialization.readVersionAndDeSerialize(this.inProgressFileRecoverableSerializer, dataInputView);
    }

    int pendingFileRecoverableSerializerVersion = dataInputView.readInt();
    int numCheckpoints = dataInputView.readInt();
    HashMap<Long, List<InProgressFileWriter.PendingFileRecoverable>> pendingFileRecoverablesPerCheckpoint = new HashMap(numCheckpoints);

    for (int i = 0; i < numCheckpoints; ++i) {
      long checkpointId = dataInputView.readLong();
      int numOfPendingFileRecoverables = dataInputView.readInt();
      List<InProgressFileWriter.PendingFileRecoverable> pendingFileRecoverables = new ArrayList(numOfPendingFileRecoverables);

      for (int j = 0; j < numOfPendingFileRecoverables; ++j) {
        byte[] bytes = new byte[dataInputView.readInt()];
        dataInputView.readFully(bytes);
        pendingFileRecoverables.add(this.pendingFileRecoverableSerializer.deserialize(pendingFileRecoverableSerializerVersion, bytes));
      }

      pendingFileRecoverablesPerCheckpoint.put(checkpointId, pendingFileRecoverables);
    }

    return new BucketState(bucketId, new Path(bucketPathStr), creationTime, current, pendingFileRecoverablesPerCheckpoint);
  }

  private SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> getResumableSerializer() {
    OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer outputStreamBasedInProgressFileRecoverableSerializer =
        (OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileRecoverableSerializer) this.inProgressFileRecoverableSerializer;
    return outputStreamBasedInProgressFileRecoverableSerializer.getResumeSerializer();
  }

  private SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> getCommitableSerializer() {
    OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverableSerializer outputStreamBasedPendingFileRecoverableSerializer =
        (OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverableSerializer) this.pendingFileRecoverableSerializer;
    return outputStreamBasedPendingFileRecoverableSerializer.getCommitSerializer();
  }

  private static void validateMagicNumber(DataInputView in) throws IOException {
    int magicNumber = in.readInt();
    if (magicNumber != 511069049) {
      throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
    }
  }
}
