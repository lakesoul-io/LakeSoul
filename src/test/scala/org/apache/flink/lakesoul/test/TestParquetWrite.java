package org.apache.flink.lakesoul.test;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.calcite.shaded.com.google.common.io.Files;
import org.apache.flink.table.api.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

public class TestParquetWrite {

//    private static final Schema SCHEMA = new Schema(
//
//            Types.NestedField.required(1, "id", Types.LONG),
//            Types.NestedField.optional(2, "data", Types.STRING),
//            Types.NestedField.optional(3, "binary", Types.STRING));




    @Before
    public void createRecords() {



    }


    @Test
    public void testDataWriter() throws IOException {


    }


}
