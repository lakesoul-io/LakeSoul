// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fury.Fury;
import org.apache.fury.ThreadLocalFury;
import org.apache.fury.config.Language;
import org.apache.fury.format.vectorized.ArrowSerializers;

import java.io.Serializable;

public class ArrowSerializer extends Serializer<VectorSchemaRoot> implements Serializable {

    ThreadLocalFury fury;

    public ArrowSerializer() {
        fury = new ThreadLocalFury(classLoader -> {
            Fury f = Fury.builder().withLanguage(Language.JAVA)
                    .withCodegen(true)
                    .requireClassRegistration(false)
                    .withClassVersionCheck(false)
                    .withStringCompressed(false)
                    .withNumberCompressed(false)
                    .withClassLoader(classLoader).build();
            ArrowSerializers.registerSerializers(f);
            return f;
        });
    }

    @Override
    public void write(Kryo kryo, Output output, VectorSchemaRoot object) {
        fury.execute(f -> {
            f.serializeJavaObject(output, object);
            return 0;
        });
    }

    @Override
    public VectorSchemaRoot read(Kryo kryo, Input input, Class<VectorSchemaRoot> type) {
        return fury.execute(f -> f.deserializeJavaObject(input.getBuffer(), VectorSchemaRoot.class));
    }
}
