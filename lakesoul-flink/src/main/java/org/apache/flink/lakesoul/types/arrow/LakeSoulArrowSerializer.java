// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types.arrow;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.lakesoul.sink.state.TableSchemaIdentitySerializer;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.IOException;

public class LakeSoulArrowSerializer extends TypeSerializer<LakeSoulArrowWrapper> {

    private static final SimpleVersionedSerializer<TableSchemaIdentity> tableSchemaIdentitySerializer = new TableSchemaIdentitySerializer();

    public LakeSoulArrowSerializer() {

    }

    /**
     * Gets whether the type is an immutable type.
     *
     * @return True, if the type is immutable.
     */
    @Override
    public boolean isImmutableType() {
        return false;
    }

    /**
     * Creates a deep copy of this serializer if it is necessary, i.e. if it is stateful. This can
     * return itself if the serializer is not stateful.
     *
     * <p>We need this because Serializers might be used in several threads. Stateless serializers
     * are inherently thread-safe while stateful serializers might not be thread-safe.
     */
    @Override
    public TypeSerializer<LakeSoulArrowWrapper> duplicate() {
        return null;
    }

    /**
     * Creates a new instance of the data type.
     *
     * @return A new instance of the data type.
     */
    @Override
    public LakeSoulArrowWrapper createInstance() {
        return null;
    }

    /**
     * Creates a deep copy of the given element in a new element.
     *
     * @param from The element reuse be copied.
     * @return A deep copy of the element.
     */
    @Override
    public LakeSoulArrowWrapper copy(LakeSoulArrowWrapper from) {
        return null;
    }

    /**
     * Creates a copy from the given element. The method makes an attempt to store the copy in the
     * given reuse element, if the type is mutable. This is, however, not guaranteed.
     *
     * @param from  The element to be copied.
     * @param reuse The element to be reused. May or may not be used.
     * @return A deep copy of the element.
     */
    @Override
    public LakeSoulArrowWrapper copy(LakeSoulArrowWrapper from, LakeSoulArrowWrapper reuse) {
        return null;
    }

    /**
     * Gets the length of the data type, if it is a fix length data type.
     *
     * @return The length of the data type, or <code>-1</code> for variable length data types.
     */
    @Override
    public int getLength() {
        return -1;
    }

    /**
     * Serializes the given record to the given target output view.
     *
     * @param record The record to serialize.
     * @param target The output view to write the serialized data to.
     * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically
     *                     raised by the output view, which may have an underlying I/O channel to which it
     *                     delegates.
     */
    @Override
    public void serialize(LakeSoulArrowWrapper record, DataOutputView target) throws IOException {
        byte[] encodedTableInfo = record.getEncodedTableInfo();
        target.writeInt(encodedTableInfo.length);
        target.write(encodedTableInfo);

        byte[] encodedBatch = record.getEncodedBatch();

        target.writeInt(encodedBatch.length);
        target.write(encodedBatch);

    }

    /**
     * De-serializes a record from the given source input view.
     *
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *                     Typically raised by the input view, which may have an underlying I/O channel from which
     *                     it reads.
     */
    @Override
    public LakeSoulArrowWrapper deserialize(DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] encodedTableInfo = new byte[len];
        source.read(encodedTableInfo);
        len = source.readInt();
        byte[] encodedBatch = new byte[len];
        source.read(encodedBatch);

        return new LakeSoulArrowWrapper(encodedTableInfo, encodedBatch);
    }

    /**
     * De-serializes a record from the given source input view into the given reuse record instance
     * if mutable.
     *
     * @param reuse  The record instance into which to de-serialize the data.
     * @param source The input view from which to read the data.
     * @return The deserialized element.
     * @throws IOException Thrown, if the de-serialization encountered an I/O related error.
     *                     Typically raised by the input view, which may have an underlying I/O channel from which
     *                     it reads.
     */
    @Override
    public LakeSoulArrowWrapper deserialize(LakeSoulArrowWrapper reuse, DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] encodedTableInfo = new byte[len];
        source.read(encodedTableInfo);
        len = source.readInt();
        byte[] encodedBatch = new byte[len];
        source.read(encodedBatch);

        return new LakeSoulArrowWrapper(encodedTableInfo, encodedBatch);
    }

    /**
     * Copies exactly one record from the source input view to the target output view. Whether this
     * operation works on binary data or partially de-serializes the record to determine its length
     * (such as for records of variable length) is up to the implementer. Binary copies are
     * typically faster. A copy of a record containing two integer numbers (8 bytes total) is most
     * efficiently implemented as {@code target.write(source, 8);}.
     *
     * @param source The input view from which to read the record.
     * @param target The target output view to which to write the record.
     * @throws IOException Thrown if any of the two views raises an exception.
     */
    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        target.writeInt(len);

        byte[] bytes = new byte[len];
        source.read(bytes);
        target.write(bytes);

        TableSchemaIdentity identity = SimpleVersionedSerialization.readVersionAndDeSerialize(
                tableSchemaIdentitySerializer, source);
        SimpleVersionedSerialization.writeVersionAndSerialize(tableSchemaIdentitySerializer, identity, target);
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    /**
     * Snapshots the configuration of this TypeSerializer. This method is only relevant if the
     * serializer is used to state stored in checkpoints/savepoints.
     *
     * <p>The snapshot of the TypeSerializer is supposed to contain all information that affects the
     * serialization format of the serializer. The snapshot serves two purposes: First, to reproduce
     * the serializer when the checkpoint/savepoint is restored, and second, to check whether the
     * serialization format is compatible with the serializer used in the restored program.
     *
     * <p><b>IMPORTANT:</b> TypeSerializerSnapshots changed after Flink 1.6. Serializers implemented
     * against Flink versions up to 1.6 should still work, but adjust to new model to enable state
     * evolution and be future-proof. See the class-level comments, section "Upgrading
     * TypeSerializers to the new TypeSerializerSnapshot model" for details.
     *
     * @return snapshot of the serializer's current configuration (cannot be {@code null}).
     * @see TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)
     */
    @Override
    public TypeSerializerSnapshot<LakeSoulArrowWrapper> snapshotConfiguration() {
        return new LakeSoulArrowTypeSerializerSnapshot();
    }
}
