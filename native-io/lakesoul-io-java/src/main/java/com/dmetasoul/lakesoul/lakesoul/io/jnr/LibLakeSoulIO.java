// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import jnr.ffi.Memory;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.LongLong;

public interface LibLakeSoulIO {

    static Pointer buildStringPointer(LibLakeSoulIO lib, String s) {
        Pointer str = Memory.allocate(Runtime.getRuntime(lib), s.length());
        str.put(0, s.getBytes(),0,s.length());

        return str;
    }

    Pointer new_tokio_runtime_builder();

    Pointer tokio_runtime_builder_set_thread_num(Pointer builder, int thread_num);

    Pointer create_tokio_runtime_from_builder(Pointer builder);

    Pointer new_lakesoul_io_config_builder();

    Pointer lakesoul_config_builder_add_single_file(Pointer builder, Pointer file);

    Pointer lakesoul_config_builder_add_single_primary_key(Pointer builder, Pointer pk);

    Pointer lakesoul_config_builder_add_single_column(Pointer builder, Pointer column);

    Pointer lakesoul_config_builder_add_single_aux_sort_column(Pointer builder, Pointer column);

    Pointer lakesoul_config_builder_add_filter(Pointer builder, Pointer filter);

    Pointer lakesoul_config_builder_add_merge_op(Pointer builder, Pointer field, Pointer mergeOp);

    Pointer lakesoul_config_builder_set_schema(Pointer builder, @LongLong long schemaAddr);

    Pointer lakesoul_config_builder_set_object_store_option(Pointer builder, Pointer key, Pointer value);

    Pointer lakesoul_config_builder_set_thread_num(Pointer builder, int thread_num);

    Pointer lakesoul_config_builder_set_batch_size(Pointer builder, int batch_size);

    Pointer lakesoul_config_builder_set_buffer_size(Pointer builder, int buffer_size);

    Pointer lakesoul_config_builder_set_max_row_group_size(Pointer builder, int row_group_size);

    Pointer create_lakesoul_io_config_from_builder(Pointer builder);

    Pointer create_lakesoul_reader_from_config(Pointer config, Pointer runtime);

    Pointer check_reader_created(Pointer reader);

    void lakesoul_reader_get_schema(Pointer reader, @LongLong long schemaAddr);

    Pointer create_lakesoul_writer_from_config(Pointer config, Pointer runtime);

    Pointer check_writer_created(Pointer writer);

    Pointer lakesoul_config_builder_set_default_column_value(Pointer ioConfigBuilder, String column, String value);

    interface BooleanCallback { // type representing callback
        @Delegate
        void invoke(Boolean status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    interface IntegerCallback { // type representing callback
        @Delegate
        void invoke(Integer status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    void start_reader(Pointer reader, BooleanCallback callback);

    void next_record_batch(Pointer reader, @LongLong long schemaAddr, @LongLong long arrayAddr, IntegerCallback callback);

    void write_record_batch(Pointer writer, @LongLong long schemaAddr, @LongLong long arrayAddr, BooleanCallback callback);

    void free_lakesoul_reader(Pointer reader);

    void flush_and_close_writer(Pointer writer, BooleanCallback callback);

    void abort_and_close_writer(Pointer writer, BooleanCallback callback);

    void free_tokio_runtime(Pointer runtime);
}
