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

public class SimpleLakeSoulPendingSplitsSerializer implements SimpleVersionedSerializer<LakeSoulPendingSplits> {
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));
    private static final int VERSION = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(LakeSoulPendingSplits splits) throws IOException {
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        List<LakeSoulSplit> lsplits = splits.getSplits();
        out.writeLong(splits.getLastReadTimestamp());
        out.writeInt(lsplits.size());
        for (LakeSoulSplit split : lsplits) {
            out.writeUTF(split.splitId());
            List<Path> paths = split.getFiles();
            out.writeInt(paths.size());
            for (Path path : paths) {
                path.write(out);
            }
            out.writeLong(split.getSkipRecord());
            out.writeInt(split.getBucketId());
        }
        out.writeUTF(splits.getTableid());
        out.writeUTF(splits.getParDesc());
        out.writeLong(splits.getDiscoverInterval());
        out.writeInt(splits.getHashBucketNum());
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public LakeSoulPendingSplits deserialize(int version, byte[] serialized) throws IOException {
        if (version == 2) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final long startReadTime = in.readLong();
            final int splitSize = in.readInt();
            final LakeSoulSplit[] lsplits = new LakeSoulSplit[splitSize];
            for (int j = 0; j < splitSize; j++) {
                final String id = in.readUTF();
                final int size = in.readInt();
                final Path[] paths = new Path[size];
                for (int i = 0; i < size; i++) {
                    paths[i] = new Path();
                    paths[i].read(in);
                }
                final long skipRecord = in.readLong();
                final int bucketID = in.readInt();
                lsplits[j] = new LakeSoulSplit(id, Arrays.asList(paths), skipRecord, bucketID);
            }
            final String tableid = in.readUTF();
            final String parDesc = in.readUTF();
            final long discoverInterval = in.readLong();
            final int hashBucketNum = in.readInt();
            return new LakeSoulPendingSplits(Arrays.asList(lsplits), startReadTime, tableid, parDesc, discoverInterval, hashBucketNum);
        }
        throw new IOException("Unknown version: " + version);
    }
}
