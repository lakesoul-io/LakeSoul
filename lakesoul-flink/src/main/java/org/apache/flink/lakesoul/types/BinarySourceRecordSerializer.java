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
        fury.getCurrentFury().serializeJavaObject(output, object);
    }

    @Override public BinarySourceRecord read(Kryo kryo, Input input, Class<BinarySourceRecord> type) {
        return fury.getCurrentFury().deserializeJavaObject(input, BinarySourceRecord.class);
    }
}
