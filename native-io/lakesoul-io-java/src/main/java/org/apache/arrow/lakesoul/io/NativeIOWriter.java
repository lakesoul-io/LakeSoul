/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.lakesoul.io;

import jnr.ffi.Pointer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class NativeIOWriter extends NativeIOBase implements AutoCloseable {

    private Pointer writer = null;

    public NativeIOWriter(Schema schema) {
        super("NativeWriter");
        setSchema(schema);
    }

    public void setPrimaryKeys(Iterable<String> primaryKeys) {
        for (String pk : primaryKeys) {
            Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, pk);
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_primary_key(ioConfigBuilder, ptr);
        }
    }

    public void setAuxSortColumns(Iterable<String> auxSortColumns) {
        for (String col : auxSortColumns) {
            Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, col);
            ioConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_aux_sort_column(ioConfigBuilder, ptr);
        }
    }

    public void initializeWriter() throws IOException {
        assert tokioRuntimeBuilder != null;
        assert ioConfigBuilder != null;

        tokioRuntime = libLakeSoulIO.create_tokio_runtime_from_builder(tokioRuntimeBuilder);
        config = libLakeSoulIO.create_lakesoul_io_config_from_builder(ioConfigBuilder);
        writer = libLakeSoulIO.create_lakesoul_writer_from_config(config, tokioRuntime);
        // tokioRuntime will be moved to reader, we don't need to free it
        tokioRuntime = null;
        Pointer p = libLakeSoulIO.check_writer_created(writer);
        if (p != null) {
            writer = null;
            throw new IOException("Init native writer failed with error: " + p.getString(0));
        }
    }

    public void write(VectorSchemaRoot batch) throws IOException {
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema schema = ArrowSchema.allocateNew(allocator);
        Data.exportVectorSchemaRoot(allocator, batch, provider, array, schema);
        AtomicReference<String> errMsg = new AtomicReference<>();
        libLakeSoulIO.write_record_batch(writer, schema.memoryAddress(), array.memoryAddress(), (status, err) -> {
            array.close();
            schema.close();
            if (!status && err != null) {
                errMsg.set(err);
            }
        });
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer write batch failed with error: " + errMsg.get());
        }
    }

    public void flush() throws IOException {
        AtomicReference<String> errMsg = new AtomicReference<>();
        libLakeSoulIO.flush_and_close_writer(writer, (status, err) -> {
            if (!status && err != null) {
                errMsg.set(err);
            }
        });
        writer = null;
        if (errMsg.get() != null && !errMsg.get().isEmpty()) {
            throw new IOException("Native writer flush failed with error: " + errMsg.get());
        }
    }

    @Override
    public void close() throws Exception {
        if (writer != null) {
            flush();
        }
        super.close();
    }
}
