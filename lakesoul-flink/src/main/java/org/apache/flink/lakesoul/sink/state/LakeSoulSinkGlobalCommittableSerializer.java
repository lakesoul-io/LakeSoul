// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Versioned serializer for {@link LakeSoulMultiTableSinkGlobalCommittable}.
 */
public class LakeSoulSinkGlobalCommittableSerializer
        implements SimpleVersionedSerializer<LakeSoulMultiTableSinkGlobalCommittable> {

    private static final int MAGIC_NUMBER = 0x1e765c80;

    private final LakeSoulSinkCommittableSerializer
            committableSerializer;

    public LakeSoulSinkGlobalCommittableSerializer(
            LakeSoulSinkCommittableSerializer
                    committableSerializer) {
        this.committableSerializer = checkNotNull(committableSerializer);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(LakeSoulMultiTableSinkGlobalCommittable committable) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(committable, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public LakeSoulMultiTableSinkGlobalCommittable deserialize(int version,
                                                               byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private void serializeV1(LakeSoulMultiTableSinkGlobalCommittable globalCommittable,
                             DataOutputView dataOutputView)
            throws IOException {
        Map<Tuple2<TableSchemaIdentity, String>, List<LakeSoulMultiTableSinkCommittable>> groupedCommittable =
                globalCommittable.getGroupedCommittable();
        assert groupedCommittable != null;
        List<LakeSoulMultiTableSinkCommittable> committableList =
                groupedCommittable.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        dataOutputView.writeInt(committableList.size());
        for (LakeSoulMultiTableSinkCommittable committable : committableList) {
            SimpleVersionedSerialization.writeVersionAndSerialize(committableSerializer, committable, dataOutputView);
        }
    }

    private LakeSoulMultiTableSinkGlobalCommittable deserializeV1(DataInputView dataInputView) throws IOException {
        List<LakeSoulMultiTableSinkCommittable> committables = new ArrayList<>();
        int size = dataInputView.readInt();
        if (size > 0) {
            for (int i = 0; i < size; ++i) {
                committables.add(
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                committableSerializer, dataInputView));
            }
        }
        return LakeSoulMultiTableSinkGlobalCommittable.fromLakeSoulMultiTableSinkCommittable(committables, false);
    }
}
