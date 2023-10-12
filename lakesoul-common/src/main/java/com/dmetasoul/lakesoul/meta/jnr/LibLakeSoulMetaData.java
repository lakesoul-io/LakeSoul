// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import jnr.ffi.Pointer;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.LongLong;

public interface LibLakeSoulMetaData {

    Pointer create_tokio_runtime();

    void free_tokio_runtime(Pointer runtime);

    Pointer create_prepared_statement();

    void free_prepared_statement(Pointer prepared);

    Pointer create_tokio_postgres_client(BooleanCallback booleanCallback, String config, Pointer runtime);

    void free_tokio_postgres_client(Pointer client);

    Pointer execute_query(IntegerCallback integerCallback, Pointer runtime, Pointer client, Pointer prepared, Integer type, String texts);

    void export_bytes_result(BooleanCallback booleanCallback, Pointer bytes, Integer len, @LongLong long addr);

    void free_bytes_result(Pointer bytes);

    void execute_update(IntegerCallback integerCallback, Pointer runtime, Pointer client, Pointer prepared, Integer type, String texts);

    void execute_query_scalar(StringCallback stringCallback, Pointer runtime, Pointer client, Pointer prepared, Integer type, String texts);

    void execute_insert(IntegerCallback integerCallback, Pointer runtime, Pointer client, Pointer prepared, Integer type, @LongLong long addr, int length);

    void clean_meta_for_test(IntegerCallback integerCallback, Pointer runtime, Pointer client);

    void hello_world(Callback<byte[]> bytesCallback);

    void namespace(byte[] bytes, Integer len);

    Pointer create_native_client(
            BooleanCallback booleanCallback,
            String config);

    interface Callback<T> {
        @Delegate
        void invoke(T result, String msg);
    }

    interface VoidCallback { // type representing callback
        @Delegate
        void invoke(String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    interface BooleanCallback { // type representing callback
        @Delegate
        void invoke(Boolean status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    interface IntegerCallback { // type representing callback
        @Delegate
        void invoke(Integer status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    interface StringCallback { // type representing callback
        @Delegate
        void invoke(String status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }


}
