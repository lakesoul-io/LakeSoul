package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;


public class NativeWriterTest {

    @Test
    public void check_writer_test() {
        Schema testSchema = new Schema(Collections.emptyList());

        NativeIOWriter writer = new NativeIOWriter(testSchema);

        // this should raise an error on the native side
        writer.withPrefix("error://xxxxx");

        try {
            writer.initializeWriter();
            throw new AssertionError("Expected IOException to be thrown");
        } catch (IOException e) {
        }
    }
}