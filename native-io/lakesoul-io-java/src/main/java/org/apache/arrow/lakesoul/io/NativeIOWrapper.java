package org.apache.arrow.lakesoul.io;

import jnr.ffi.LibraryLoader;
import jnr.ffi.LibraryOption;
import jnr.ffi.ObjectReferenceManager;
import jnr.ffi.Pointer;
import jnr.ffi.Runtime;
import org.apache.arrow.lakesoul.io.jnr.LibLakeSoulIO;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;



public class NativeIOWrapper implements AutoCloseable {
    protected Pointer readerConfigBuilder, reader, config, tokioRuntimeBuilder, tokioRuntime;
    protected LibLakeSoulIO libLakeSoulIO;
    protected final ObjectReferenceManager referenceManager;

    public static boolean isMac() {
        String OS = System.getProperty("os.name").toLowerCase();
        return (OS.indexOf("mac") >= 0);

    }
    public NativeIOWrapper(){
        Map<LibraryOption, Object> libraryOptions = new HashMap<>();
        libraryOptions.put(LibraryOption.LoadNow, true);
        libraryOptions.put(LibraryOption.IgnoreError, true);

        String ext = ".dylib";
        if (!isMac()) {
            ext = ".so";
        }

        String libName = String.join("/", System.getenv("LakeSoulLib"),"liblakesoul_io_c" + ext); // platform specific name for liblakesoul_io_c
        libLakeSoulIO = LibraryLoader.loadLibrary(
                LibLakeSoulIO.class,
                libraryOptions,
                libName
        );
        referenceManager = Runtime.getRuntime(libLakeSoulIO).newObjectReferenceManager();
    }
    public void initialize(){
        readerConfigBuilder = libLakeSoulIO.new_lakesoul_reader_config_builder();
        tokioRuntimeBuilder = libLakeSoulIO.new_tokio_runtime_builder();
        setBufferSize(1);
        setThreadNum(2);
    }

    public void addFile(String file){
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, file);
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_file(readerConfigBuilder, ptr);
    }

    public void addColumn(String column){
        Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, column);
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_single_column(readerConfigBuilder, ptr);
    }

    public void addFilter(String filter){
        if (!useJavaReader) {
            assert readerConfigBuilder != null;
            System.out.println("[JNI][JAVA]addFilter "+filter);
            Pointer ptr = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, filter);
            readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_add_filter(readerConfigBuilder, ptr);
        }
    }


    public void setThreadNum(int threadNum){
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_thread_num(readerConfigBuilder, threadNum);
    }

    public void setBatchSize(int batchSize){
        readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_batch_size(readerConfigBuilder, batchSize);
    }

    public void setBufferSize(int bufferSize){
        if (!useJavaReader) {
            readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_buffer_size(readerConfigBuilder, bufferSize);
        }
    }

    public void setObjectStoreOptions(String accessKey, String accessSecret, String region, String bucketName, String endpoint){
        setObjectStoreOption("fs.s3.enabled", "true");
        setObjectStoreOption("fs.s3.access.key", accessKey);
        setObjectStoreOption("fs.s3.access.secret", accessSecret);
        setObjectStoreOption("fs.s3.region", region);
        setObjectStoreOption("fs.s3.bucket", bucketName);
        setObjectStoreOption("fs.s3.endpoint", endpoint);
    }

    public void setObjectStoreOption(String key, String value){
        if (!useJavaReader) {
            Pointer ptrKey = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, key);
            Pointer ptrValue = LibLakeSoulIO.buildStringPointer(libLakeSoulIO, value);
            readerConfigBuilder = libLakeSoulIO.lakesoul_config_builder_set_object_store_option(readerConfigBuilder, ptrKey, ptrValue);
        }
    }

    public void createReader(){
        tokioRuntime = libLakeSoulIO.create_tokio_runtime_from_builder(tokioRuntimeBuilder);

        config = libLakeSoulIO.create_lakesoul_reader_config_from_builder(readerConfigBuilder);
        reader = libLakeSoulIO.create_lakesoul_reader_from_config(config, tokioRuntime);
    }

    @Override
    public void close() throws Exception {
    }

    public static final class Callback implements LibLakeSoulIO.JavaCallback {

        public Consumer<Boolean> callback;
        public long array_ptr;
        private Pointer key;
        private ObjectReferenceManager referenceManager;

        public Callback(Consumer<Boolean> callback) {
            this(callback, 0L);
        }

        public Callback(Consumer<Boolean> callback, long array_ptr) {
            this.callback = callback;
            this.array_ptr = array_ptr;
        }

        public Callback(Consumer<Boolean> callback, ObjectReferenceManager referenceManager) {
            this.callback = callback;
            this.referenceManager = referenceManager;
            key = null;
        }

        public void registerReferenceKey() {
            key = referenceManager.add(this);
        }

        public void removerReferenceKey() {
            if (key!= null) {
                referenceManager.remove(key);
            }
        }

        @Override
        public void invoke(boolean status, String err) {
            callback.accept(status);
            removerReferenceKey();
        }
    }

    public void startReader(Consumer<Boolean> callback) {
        libLakeSoulIO.start_reader(reader, new Callback(callback));
    }


    public void nextBatch(Consumer<Boolean> callback, long schemaAddr, long arrayAddr){
        Callback nativeCallback = new Callback(callback, referenceManager);
        nativeCallback.registerReferenceKey();
        libLakeSoulIO.next_record_batch(reader, schemaAddr, arrayAddr, nativeCallback);

        // next_record_batch will time out when gc is called  before invoking callback
//        System.gc();

    }

    public void free_lakesoul_reader(){
        libLakeSoulIO.free_lakesoul_reader(reader);
    }

}
