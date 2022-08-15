package org.apache.arrow.lakesoul.io.jnr;

import jnr.ffi.Memory;
import jnr.ffi.NativeType;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import jnr.ffi.annotations.Delegate;

import java.util.List;

public interface LibLakeSoulIO {
    static Pointer buildArrayStringPointer(LibLakeSoulIO lib, List<String> files) {
        Pointer memory =  Memory.allocateDirect(Runtime.getRuntime(lib), NativeType.ADDRESS);
        Pointer str = Memory.allocate(Runtime.getRuntime(lib), files.get(0).length());
        str.put(0, files.get(0).getBytes(),0,files.get(0).length());
        memory.putAddress(0, str.address());
        System.out.println(memory.arrayLength());
//        memory.put(0, addrs, 0, 1);

        return memory;
    }

    static Pointer buildStringPointer(LibLakeSoulIO lib, String s) {
        Pointer str = Memory.allocate(Runtime.getRuntime(lib), s.length());
        str.put(0, s.getBytes(),0,s.length());

        return str;
    }

    Pointer new_lakesoul_reader_config_builder();

    Pointer lakesoul_config_builder_add_single_file(Pointer builder, Pointer file);

    Pointer lakesoul_config_builder_add_single_column(Pointer builder, Pointer column);

    Pointer lakesoul_config_builder_add_file(Pointer builder, Pointer files, int file_num);

    Pointer lakesoul_config_builder_set_thread_num(Pointer builder, int thread_num);

    Pointer create_lakesoul_reader_config_from_builder(Pointer builder);

    Pointer create_lakesoul_reader_from_config(Pointer config);

    public static interface JavaCallback { // type representing callback
        @Delegate
        void invoke(boolean status, String err); // function name doesn't matter, it just needs to be the only function and have @Delegate
    }

    void start_reader(Pointer reader, JavaCallback callback);

    void next_record_batch(Pointer reader, long schemaAddr, long arrayAddr, JavaCallback callback);

    void free_lakesoul_reader(Pointer reader);
}
