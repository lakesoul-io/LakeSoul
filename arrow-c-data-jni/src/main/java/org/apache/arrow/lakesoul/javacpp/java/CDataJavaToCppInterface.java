package org.apache.arrow.lakesoul.javacpp.java;

import org.apache.arrow.ArrowUtils;
import org.bytedeco.javacpp.FunctionPointer;
import org.bytedeco.javacpp.Loader;
import org.bytedeco.javacpp.Pointer;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

import java.util.function.Consumer;

//@Properties(
//        target = "org.apache.arrow.lakesoul.javacpp.java.CDataJavaToCppInterface",
//        value = @Platform(
//                include = {
//                        "CDataCppBridge.h"
//                },
//                compiler = {"cpp11"},
//                linkpath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/debug/"},
//                includepath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/src", ArrowUtils.LOCAL_ARROW_DIR + "/cpp/src", ArrowUtils.LOCAL_BASE_DIR+"/src/main/java/org/apache/arrow/lakesoul/javacpp/cpp"},
//                link = {"arrow"}
//        )
//)
@Platform(
        include = {
                "CDataCppBridge.h"
        },
        compiler = {"cpp11"},
        linkpath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/debug/"},
        includepath = {ArrowUtils.LOCAL_ARROW_DIR + "/cpp/build/src", ArrowUtils.LOCAL_ARROW_DIR + "/cpp/src", ArrowUtils.LOCAL_BASE_DIR+"/src/main/java/org/apache/arrow/lakesoul/javacpp/cpp"},
        link = {"arrow"}
)
public class CDataJavaToCppInterface implements InfoMapper {

    @Override
    public void map(InfoMap infoMap) {
    }

    static { Loader.load(); }

// Parsed from CDataCppBridge.h

// #include <iostream>
// #include <arrow/api.h>
// #include <arrow/c/bridge.h>

    public static class Foo extends Pointer {
        static { Loader.load(); }
        public Foo(int n) { allocate(n); }
//        public Foo(int n, Consumer<Boolean> consumer) { allocate(n); this.consumer = consumer;}
        private native void allocate(int n);

        public Consumer<Boolean> consumer;
        @NoOffset
        public native int n();
        public native Foo n(int n);
        @Virtual  public native void bar();



    }




    public static native void FillInt64Array(@Cast("const uintptr_t") long c_schema_ptr, @Cast("const uintptr_t") long c_array_ptr);

    public static native void FillInt64ArrayWithCallBack(@Cast("const uintptr_t") long c_schema_ptr, @Cast("const uintptr_t") long c_array_ptr, Foo foo);

    public static native void FillInt64Batch(@Cast("const uintptr_t") long c_schema_ptr, @Cast("const uintptr_t") long c_array_ptr);

}
