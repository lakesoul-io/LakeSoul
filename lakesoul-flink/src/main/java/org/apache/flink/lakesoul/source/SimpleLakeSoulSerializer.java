// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SimpleLakeSoulSerializer implements SimpleVersionedSerializer<LakeSoulPartitionSplit> {
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LakeSoulPartitionSplit split) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeUTF(split.splitId());
        List<Path> paths = split.getFiles();
        out.writeInt(paths.size());
        for (Path path : paths) {
            path.write(out);
        }
        out.writeLong(split.getSkipRecord());
        out.writeInt(split.getBucketId());
        out.writeUTF(split.getPartitionDesc());
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }


    @Override
    public LakeSoulPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);

            final String id = in.readUTF();
            final int size = in.readInt();
            final Path[] paths = new Path[size];
            for (int i = 0; i < size; i++) {
                paths[i] = new Path();
                paths[i].read(in);
            }
            final long skipRecord = in.readLong();
            final int bucketid = in.readInt();
            final String partitionDesc = in.readUTF();
            return new LakeSoulPartitionSplit(id, Arrays.asList(paths), skipRecord, bucketid, partitionDesc);
        }
        throw new IOException("Unknown version: " + version);
    }
}
