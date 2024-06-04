// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types.arrow;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class LakeSoulArrowTypeSerializerSnapshot implements TypeSerializerSnapshot<LakeSoulArrowWrapper> {
    public LakeSoulArrowTypeSerializerSnapshot() {
    }

    /**
     * Returns the version of the current snapshot's written binary format.
     *
     * @return the version of the current snapshot's written binary format.
     */
    @Override
    public int getCurrentVersion() {
        return 0;
    }

    /**
     * Writes the serializer snapshot to the provided {@link DataOutputView}. The current version of
     * the written serializer snapshot's binary format is specified by the {@link
     * #getCurrentVersion()} method.
     *
     * @param out the {@link DataOutputView} to write the snapshot to.
     * @throws IOException Thrown if the snapshot data could not be written.
     * @see #writeVersionedSnapshot(DataOutputView, TypeSerializerSnapshot)
     */
    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {

    }

    /**
     * Reads the serializer snapshot from the provided {@link DataInputView}. The version of the
     * binary format that the serializer snapshot was written with is provided. This version can be
     * used to determine how the serializer snapshot should be read.
     *
     * @param readVersion         version of the serializer snapshot's written binary format
     * @param in                  the {@link DataInputView} to read the snapshot from.
     * @param userCodeClassLoader the user code classloader
     * @throws IOException Thrown if the snapshot data could be read or parsed.
     * @see #readVersionedSnapshot(DataInputView, ClassLoader)
     */
    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {

    }

    /**
     * Recreates a serializer instance from this snapshot. The returned serializer can be safely
     * used to read data written by the prior serializer (i.e., the serializer that created this
     * snapshot).
     *
     * @return a serializer instance restored from this serializer snapshot.
     */
    @Override
    public TypeSerializer restoreSerializer() {
        return null;
    }

    /**
     * Checks a new serializer's compatibility to read data written by the prior serializer.
     *
     * <p>When a checkpoint/savepoint is restored, this method checks whether the serialization
     * format of the data in the checkpoint/savepoint is compatible for the format of the serializer
     * used by the program that restores the checkpoint/savepoint. The outcome can be that the
     * serialization format is compatible, that the program's serializer needs to reconfigure itself
     * (meaning to incorporate some information from the TypeSerializerSnapshot to be compatible),
     * that the format is outright incompatible, or that a migration needed. In the latter case, the
     * TypeSerializerSnapshot produces a serializer to deserialize the data, and the restoring
     * program's serializer re-serializes the data, thus converting the format during the restore
     * operation.
     *
     * @param newSerializer the new serializer to check.
     * @return the serializer compatibility result.
     */
    @Override
    public TypeSerializerSchemaCompatibility resolveSchemaCompatibility(TypeSerializer newSerializer) {
        return null;
    }
}
