package org.apache.arrow.lakesoul.io;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.lakesoul.javacpp.java.CDataJavaToCppInterface;

import java.util.function.Consumer;


public class ArrowCDataWrapper {

    private CDataJavaToCppInterface cDataInterface;

    public ArrowCDataWrapper(){
        cDataInterface = new CDataJavaToCppInterface();
    }

    public void nextArray(Consumer<Boolean> callback, long schemaAddr, long arrayAddr){
        callback.accept(nextArrayFromCDataInterface(schemaAddr, arrayAddr));
        System.out.println(ArrowSchema.wrap(schemaAddr));
        System.out.println(ArrowArray.wrap(arrayAddr));
    }

    public void nextBatch(Consumer<Boolean> callback, long schemaAddr, long arrayAddr){
        callback.accept(nextBatchFromCDataInterface(schemaAddr, arrayAddr));
        System.out.println(ArrowSchema.wrap(schemaAddr));
        System.out.println(ArrowArray.wrap(arrayAddr));
    }

    private boolean nextArrayFromCDataInterface(long schemaPtr, long arrayPtr){
//        CDataJavaToCppInterface.FillInt64Array(schemaPtr, arrayPtr);
        CDataJavaToCppInterface.FillInt64ArrayWithCallBack(schemaPtr, arrayPtr, new CDataJavaToCppInterface.Foo(2));
        return true;
    }

    private boolean nextBatchFromCDataInterface(long schemaPtr, long arrayPtr){
        CDataJavaToCppInterface.FillInt64Batch(schemaPtr, arrayPtr);
        return true;
    }

}
