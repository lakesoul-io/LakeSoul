package org.apache.flink.lakesoul.sink.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Arrays;

public class VariableLengthByteKeyComparator<IN>
        extends TypeComparator<Tuple2<byte[], StreamRecord<IN>>> {
    private byte[] keyReference;
    private long timestampReference;

    @Override
    public int hash(Tuple2<byte[], StreamRecord<IN>> record) {
        return record.hashCode();
    }

    @Override
    public void setReference(Tuple2<byte[], StreamRecord<IN>> toCompare) {
        this.keyReference = Arrays.copyOf(toCompare.f0, toCompare.f0.length);
        this.timestampReference = toCompare.f1.asRecord().getTimestamp();
    }

    @Override
    public boolean equalToReference(Tuple2<byte[], StreamRecord<IN>> candidate) {
        return Arrays.equals(keyReference, candidate.f0)
                && timestampReference == candidate.f1.asRecord().getTimestamp();
    }

    @Override
    public int compareToReference(
            TypeComparator<Tuple2<byte[], StreamRecord<IN>>> referencedComparator) {
        byte[] otherKey = ((VariableLengthByteKeyComparator<IN>) referencedComparator).keyReference;
        long otherTimestamp =
                ((VariableLengthByteKeyComparator<IN>) referencedComparator).timestampReference;

        int keyCmp = compare(otherKey, this.keyReference);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return Long.compare(otherTimestamp, this.timestampReference);
    }

    @Override
    public int compare(
            Tuple2<byte[], StreamRecord<IN>> first, Tuple2<byte[], StreamRecord<IN>> second) {
        int keyCmp = compare(first.f0, second.f0);
        if (keyCmp != 0) {
            return keyCmp;
        }
        return Long.compare(
                first.f1.asRecord().getTimestamp(), second.f1.asRecord().getTimestamp());
    }

    private int compare(byte[] first, byte[] second) {
        int firstLength = first.length;
        int secondLength = second.length;
        int minLength = Math.min(firstLength, secondLength);
        for (int i = 0; i < minLength; i++) {
            int cmp = Byte.compare(first[i], second[i]);

            if (cmp != 0) {
                return cmp;
            }
        }

        return Integer.compare(firstLength, secondLength);
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        int firstLength = firstSource.readInt();
        int secondLength = secondSource.readInt();
        int minLength = Math.min(firstLength, secondLength);
        while (minLength-- > 0) {
            byte firstValue = firstSource.readByte();
            byte secondValue = secondSource.readByte();

            int cmp = Byte.compare(firstValue, secondValue);
            if (cmp != 0) {
                return cmp;
            }
        }

        int lengthCompare = Integer.compare(firstLength, secondLength);
        if (lengthCompare != 0) {
            return lengthCompare;
        } else {
            return Long.compare(firstSource.readLong(), secondSource.readLong());
        }
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return true;
    }

    @Override
    public void putNormalizedKey(
            Tuple2<byte[], StreamRecord<IN>> record,
            MemorySegment target,
            int offset,
            int numBytes) {
        BytesKeyNormalizationUtil.putNormalizedKey(
                record, record.f0.length, target, offset, numBytes);
    }

    @Override
    public boolean invertNormalizedKey() {
        return false;
    }

    @Override
    public TypeComparator<Tuple2<byte[], StreamRecord<IN>>> duplicate() {
        return new VariableLengthByteKeyComparator<>();
    }

    @Override
    public int extractKeys(Object record, Object[] target, int index) {
        target[index] = record;
        return 1;
    }

    @Override
    public TypeComparator<?>[] getFlatComparators() {
        return new TypeComparator[] {this};
    }

    // --------------------------------------------------------------------------------------------
    // unsupported normalization
    // --------------------------------------------------------------------------------------------

    @Override
    public boolean supportsSerializationWithKeyNormalization() {
        return false;
    }

    @Override
    public void writeWithKeyNormalization(
            Tuple2<byte[], StreamRecord<IN>> record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Tuple2<byte[], StreamRecord<IN>> readWithKeyDenormalization(
            Tuple2<byte[], StreamRecord<IN>> reuse, DataInputView source) throws IOException {
        throw new UnsupportedOperationException();
    }
}
