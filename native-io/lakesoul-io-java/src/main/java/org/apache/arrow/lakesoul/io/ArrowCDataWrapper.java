package org.apache.arrow.lakesoul.io;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.Pointer;
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;

import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;



public class ArrowCDataWrapper {

    protected Pointer readerConfigBuilder, reader, config;
    protected LibLakeSoulIO libLakeSoulIO;


    public ArrowCDataWrapper(){
        Map<LibraryOption, Object> libraryOptions = new HashMap<>();
        libraryOptions.put(LibraryOption.LoadNow, true);
        libraryOptions.put(LibraryOption.IgnoreError, true);

        String libName = String.join("/", System.getenv("LakeSoulLib"),"liblakesoul_io_c.so"); // platform specific name for liblakesoul_io_c
//        String libName ="/Users/ceng/local/lib/lakesoul/liblakesoul_io_c.dylib";
        System.out.println(System.getenv("LakeSoulLib"));
        System.out.println(System.getenv("lakesoul_home"));
        System.out.println(libName);
        libLakeSoulIO = LibraryLoader.loadLibrary(
                LibLakeSoulIO.class,
                libraryOptions,
                libName
        );
    }

    public void initializeConfigBuilder(){
        readerConfigBuilder = libLakeSoulIO.new_lakesoul_reader_config_builder();
    }

    public void addFile(String file){
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, file);
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_file(readerConfigBuilder, ptr);
    }

    public void addColumn(String column){
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, column);
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_column(readerConfigBuilder, ptr);
    }

    public void setThreadNum(int threadNum){
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_thread_num(readerConfigBuilder, threadNum);
    }

    public void addFiles(List<String> files, int file_num){
        Pointer ptr = LibLakeSoulIO.buildArrayStringPointer(libLakeSoulIO, files);
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_file(readerConfigBuilder, ptr ,file_num);
    }

    public void createReader(){
        config = libLakeSoulIO.create_lakesoul_reader_config_from_builder(readerConfigBuilder);
        reader = libLakeSoulIO.create_lakesoul_reader_from_config(config);
    }

    public static final class Callback implements LibLakeSoulIO.JavaCallback {

        public Consumer<Boolean> callback;
        public long array_ptr;

        public Callback(Consumer<Boolean> callback) {
            this(callback, 0L);
        }

        public Callback(Consumer<Boolean> callback, long array_ptr) {
            this.callback = callback;
            this.array_ptr = array_ptr;
        }

        @Override
        public void invoke(boolean status, String err) {
            System.out.println("[From Java][org.apache.arrow.lakesoul.io.ArrowCDataWrapper.Callback.invoke] status=" +status +" , errMsg="+err);
            callback.accept(status);
        }
    }

    public void startReader(Consumer<Boolean> callback) {
        libLakeSoulIO.start_reader(reader, new Callback(callback));
    }


    public void nextBatch(Consumer<Boolean> callback, long schemaAddr, long arrayAddr){

        libLakeSoulIO.next_record_batch(reader, schemaAddr, arrayAddr, new Callback(callback));
    }

    public void free_lakesoul_reader(){
        System.out.println("[From Java][org.apache.arrow.lakesoul.io.ArrowCDataWrapper.free_lakesoul_reader] method called");
        libLakeSoulIO.free_lakesoul_reader(reader);
    }

}
