package org.apache.arrow.lakesoul.read;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.HashMap;

public class LakeSoulArrowReader {
    /**
     * native method for creating LakeSoulArrowReader
     * @param files
     * @param primary_keys
     * @param columns
     * @param filters
     * @param object_store_options
     * @return
     */
    public static native LakeSoulArrowReader createLakeSoulArrowReader(String[] files,
                                                                       String[] primary_keys,
                                                                       String[] columns,
                                                                       String[] filters,
                                                                       HashMap<String, String> object_store_options);

    /**
     * native method to get ArrowArray memory address
     * @return
     */
    public native long getArrowArrayAddress();

    /**
     * native method to get ArrowSchema memory address
     * @return
     */
    public native long getArrowSchemaAddress();

    private final BufferAllocator allocator = new RootAllocator();
    private ArrowSchema arrowSchema = null;

    public boolean next() {
        throw new RuntimeException("next() Not implemented");
    }

    private void importArrowSchema(){
        long memoryAddress = this.getArrowSchemaAddress();
        arrowSchema = ArrowSchema.wrap(memoryAddress);
    }

    private ArrowSchema getArrowSchema(){
        if (arrowSchema == null) {
            importArrowSchema();
        }
        return arrowSchema;
    }

    /**
     * Public Java Interface for getting next VectorSchemaRoot
     * @return next VectorSchemaRoot
     */
    public VectorSchemaRoot getVectorSchemaRoot(){
        return this.getVectorSchemaRootByAddr();
    }

    private VectorSchemaRoot getVectorSchemaRootByAddr(){

        long memoryAddress = this.getArrowArrayAddress();
        ArrowArray importedArrowArray = ArrowArray.wrap(memoryAddress);

        ArrowSchema importedArrowSchema = this.getArrowSchema();

        return Data.importVectorSchemaRoot(allocator, importedArrowArray, importedArrowSchema, null);
    }
}

