// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.meta.jnr;

import com.google.protobuf.InvalidProtocolBufferException;
import jnr.ffi.Pointer;
import jnr.ffi.annotations.Delegate;
import jnr.ffi.annotations.LongLong;

public interface LibLakeSoulMetaData {

    void execute_query(IntegerCallback integerCallback, Pointer client, String type, String texts, String delim, @LongLong long addr);

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

    interface BytesCallback { // type representing callback
        @Delegate
        void invoke(byte[] status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }


}
