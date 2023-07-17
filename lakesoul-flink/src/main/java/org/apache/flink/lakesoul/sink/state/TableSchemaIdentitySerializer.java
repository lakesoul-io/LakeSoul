// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.state;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TableSchemaIdentitySerializer
        implements SimpleVersionedSerializer<TableSchemaIdentity> {

    private final Kryo kryo = new Kryo();

    public TableSchemaIdentitySerializer() {
        kryo.setClassLoader(this.getClass().getClassLoader());
        kryo.register(TableSchemaIdentity.class, new JavaSerializer<TableSchemaIdentity>());
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(TableSchemaIdentity obj) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        Output output = new Output(4096, 1024 * 1024);
        kryo.writeClassAndObject(output, obj);
        out.write(output.toBytes());
        output.close();

        return out.getCopyOfBuffer();
    }

    @Override
    public TableSchemaIdentity deserialize(int version, byte[] serialized) throws IOException {
        Input input = new Input(new ByteArrayInputStream(serialized));
        return (TableSchemaIdentity) kryo.readClassAndObject(input);
    }
}
