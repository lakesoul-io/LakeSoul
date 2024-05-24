// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.io.jnr;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.LongLong;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.IntByReference;

public interface LibLakeSoulIO {

    Pointer new_tokio_runtime_builder();

    Pointer create_tokio_runtime_from_builder(Pointer builder);

    Pointer new_lakesoul_io_config_builder();

    Pointer lakesoul_config_builder_add_single_file(Pointer builder, String file);

    Pointer lakesoul_config_builder_with_prefix(Pointer builder, String file);

    Pointer lakesoul_config_builder_set_hash_bucket_num(Pointer builder, int hash_bucket_num);

    Pointer lakesoul_config_builder_add_single_primary_key(Pointer builder, String pk);

    Pointer lakesoul_config_builder_add_single_range_partition(Pointer builder, String col);

    Pointer lakesoul_config_builder_add_single_column(Pointer builder, String column);

    Pointer lakesoul_config_builder_add_single_aux_sort_column(Pointer builder, String column);

    Pointer lakesoul_config_builder_add_filter(Pointer builder, String filter);

    Pointer lakesoul_config_builder_add_filter_proto(Pointer builder, @LongLong long proto_addr, int len);

    Pointer lakesoul_config_builder_add_merge_op(Pointer builder, String field, String mergeOp);

    Pointer lakesoul_config_builder_set_schema(Pointer builder, @LongLong long schemaAddr);

    Pointer lakesoul_config_builder_set_partition_schema(Pointer builder, @LongLong long schemaAddr);

    Pointer lakesoul_config_builder_set_object_store_option(Pointer builder, String key, String value);

    Pointer lakesoul_config_builder_set_thread_num(Pointer builder, int thread_num);

    Pointer lakesoul_config_builder_set_dynamic_partition(Pointer builder, boolean enable);

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

    String next_record_batch_blocked(Pointer reader, @LongLong long arrayAddr, @Out IntByReference count);

    void write_record_batch(Pointer writer, @LongLong long schemaAddr, @LongLong long arrayAddr, BooleanCallback callback);

    String write_record_batch_blocked(Pointer writer, @LongLong long schemaAddr, @LongLong long arrayAddr);

    String write_record_batch_ipc_blocked(Pointer writer, @LongLong long schemaAddr, @LongLong long arrayAddr);

    void free_lakesoul_reader(Pointer reader);

    Pointer flush_and_close_writer(Pointer writer, IntegerCallback callback);

    void abort_and_close_writer(Pointer writer, BooleanCallback callback);

    void free_tokio_runtime(Pointer runtime);

    Pointer apply_partition_filter(IntegerCallback callback, int pbLen, long jniWrapperAddr, long schemaAddr, int filterLen, long filterAddr);

    void export_bytes_result(BooleanCallback booleanCallback, Pointer bytes, Integer len, @LongLong long addr);

    void free_bytes_result(Pointer bytes);

    void rust_logger_init();

}
