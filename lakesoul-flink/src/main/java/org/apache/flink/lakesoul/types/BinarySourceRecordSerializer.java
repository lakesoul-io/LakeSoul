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
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.binary.BinarySection;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;

public class BinarySourceRecordSerializer extends Serializer<BinarySourceRecord> implements Serializable {

    ThreadLocalFury fury;

    public BinarySourceRecordSerializer() {
        fury = Fury.builder().withLanguage(Language.JAVA)
                .withCodegen(true)
                .requireClassRegistration(false)
                .withClassVersionCheck(false)
                .withStringCompressed(false)
                .withNumberCompressed(false)
                .buildThreadLocalFury();
    }

    @Override public void write(Kryo kryo, Output output, BinarySourceRecord object) {
        fury.getCurrentFury().serialize(output, object);
    }

    @Override public BinarySourceRecord read(Kryo kryo, Input input, Class<BinarySourceRecord> type) {
        return (BinarySourceRecord) fury.getCurrentFury().deserialize(input);
    }
}
