// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fury.Fury;
import io.fury.Language;
import io.fury.ThreadLocalFury;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryFormat;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySection;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;

public class BinarySourceRecordSerializer extends Serializer<BinarySourceRecord> implements Serializable {

    ThreadLocalFury fury;

    public BinarySourceRecordSerializer() {
        fury = new ThreadLocalFury(classLoader -> {
            Fury f = Fury.builder().withLanguage(Language.JAVA)
                    .withCodegen(true)
                    .requireClassRegistration(false)
                    .withClassVersionCheck(false)
                    .withStringCompressed(false)
                    .withNumberCompressed(false)
                    .withClassLoader(classLoader).build();
            f.register(RowType.class);
            f.register(BinarySourceRecord.class);
            f.register(RowData.class);
            f.register(BinaryFormat.class);
            f.register(BinaryRowData.class);
            f.register(MemorySegment.class);
            f.register(BinarySection.class);
            f.register(TableId.class);
            f.register(LakeSoulRowDataWrapper.class);
            f.register(LogicalTypeRoot.class);
            f.register(LogicalType.class);
            f.register(RowType.RowField.class);
            f.register(ArrayType.class);
            f.register(IntType.class);
            f.register(BigIntType.class);
            f.register(BinaryType.class);
            f.register(BooleanType.class);
            f.register(VarBinaryType.class);
            f.register(CharType.class);
            f.register(VarCharType.class);
            f.register(DateType.class);
            f.register(DayTimeIntervalType.class);
            f.register(DistinctType.class);
            f.register(LocalZonedTimestampType.class);
            f.register(TimestampKind.class);
            f.register(DecimalType.class);
            f.register(DoubleType.class);
            f.register(FloatType.class);
            f.register(MapType.class);
            f.register(ArrayType.class);
            f.register(MultisetType.class);
            f.register(NullType.class);
            f.register(RawType.class);
            f.register(SmallIntType.class);
            f.register(StructuredType.class);
            f.register(SymbolType.class);
            f.register(TimestampType.class);
            f.register(TimeType.class);
            f.register(YearMonthIntervalType.class);
            f.register(ZonedTimestampType.class);
            return f;
        });
    }

    @Override public void write(Kryo kryo, Output output, BinarySourceRecord object) {
        fury.getCurrentFury().serializeJavaObject(output, object);
    }

    @Override public BinarySourceRecord read(Kryo kryo, Input input, Class<BinarySourceRecord> type) {
        return fury.getCurrentFury().deserializeJavaObject(input, BinarySourceRecord.class);
    }
}
