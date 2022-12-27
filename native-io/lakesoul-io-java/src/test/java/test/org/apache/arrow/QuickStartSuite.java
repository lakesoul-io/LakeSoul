package test.org.apache.arrow;


import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;



import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import static java.util.Arrays.asList;

/**
 * Test cases are from https://arrow.apache.org/docs/java/quickstartguide.html
 */
public class QuickStartSuite {


    /**
     * ValueVectors represent a sequence of values of the same type. They are also known as “arrays” in the columnar format.
     *
     * Example: create a vector of 32-bit integers representing [1, null, 2]:
     * Example: create a vector of UTF-8 encoded strings representing ["one", "two", "three"]:
     */
    @Test
    public void CreateValueVector() {
        try(
            BufferAllocator allocator = new RootAllocator();
            IntVector intVector = new IntVector("fixed-size-primitive-layout", allocator);
        ) {
            intVector.allocateNew(3);
//            Assert.assertEquals(intVector.getValueCount(), 0);
            intVector.set(0, 1);
//            intVector.set(1,1);
            intVector.setNull(1);
            intVector.set(2, 2);
//            Assert.assertEquals(intVector.getValueCount(), 0);
            intVector.setValueCount(3);
            System.out.println("Vector created in memory: " + intVector);
//            Assert.assertEquals(intVector.getValueCount(), 8);
//            Assert.assertEquals(intVector.getNullCount(), 6);
//
//            Assert.assertEquals(intVector.get(0), 1);
//            Assert.assertTrue(intVector.isNull(1));
//            Assert.assertEquals(intVector.get(2), 2);
            Assert.assertEquals(intVector.toString(), "[1, null, 2]");
        }
        try (

            BufferAllocator allocator_1 = new RootAllocator();
            VarCharVector varCharVector = new VarCharVector("variable-size-primitive-layout", allocator_1);
        ){
            varCharVector.allocateNew(3);
            varCharVector.set(0, "one".getBytes());
            varCharVector.set(1, "two".getBytes());
            varCharVector.set(2, "three".getBytes());
            varCharVector.setValueCount(3);
            System.out.println("Vector created in memory: " + varCharVector);
            Assert.assertEquals(varCharVector.toString(), "[one, two, three]");

        }
    }

    /**
     * Fields are used to denote the particular columns of tabular data. They consist of a name, a data type, a flag indicating whether the column can have null values, and optional key-value metadata.
     *
     * Example: create a field named “document” of string type:
     */
    @Test
    public void CreateField() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("A", "Id card");
        metadata.put("B", "Passport");
        metadata.put("C", "Visa");
        Field document = new Field("document",
                new FieldType(true, new ArrowType.Utf8(), null, metadata), null);
        System.out.println("Field created: " + document + ", Metadata: " + document.getMetadata());
        Assert.assertEquals(document.toString(), "document: Utf8");
        Assert.assertEquals(document.getMetadata().toString(), "{A=Id card, B=Passport, C=Visa}");
    }

    /**
     * Schemas hold a sequence of fields together with some optional metadata.
     *
     * Example: Create a schema describing datasets with two columns: an int32 column “A” and a UTF8-encoded string column “B”
     */
    @Test
    public void CreateSchema() {

        Map<String, String> metadata = new HashMap<>();
        metadata.put("K1", "V1");
        metadata.put("K2", "V2");
        Field a = new Field("A", FieldType.nullable(new ArrowType.Int(32, true)),null);
        Field b = new Field("B", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(asList(a, b), metadata);
        System.out.println("Schema created: " + schema);
        Assert.assertEquals(schema.toString(), "Schema<A: Int(32, true), B: Utf8>(metadata: {K1=V1, K2=V2})");
    }

    /**
     * A VectorSchemaRoot combines ValueVectors with a Schema to represent tabular data.
     *
     * Example: Create a dataset of names (strings) and ages (32-bit signed integers).
     */
    @Test
    public void CreateVectorSchemaRoot() {
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(asList(age, name), null);

        try (
            BufferAllocator allocator = new RootAllocator();
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            IntVector ageVector = (IntVector) root.getVector("age");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
        ){
            root.setRowCount(3);
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);
            nameVector.allocateNew(3);
            nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
            nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
            nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));
            System.out.println("VectorSchemaRoot created: \n" + root.contentToTSVString());
            Assert.assertEquals(root.contentToTSVString(),
                    "age\tname\n" +
                        "10\tDave\n" +
                        "20\tPeter\n" +
                        "30\tMary\n");
        }
    }

    /**
     * Arrow data can be written to and read from disk, and both of these can be done in a streaming and/or random-access fashion depending on application requirements.
     *
     * Write data to an arrow file
     *
     * Example: Write the dataset from the previous example to an Arrow random-access file.
     *
     * Read data from an arrow file
     *
     * Example: Read the dataset from the previous example to an Arrow random-access file.
     */
    @Test
    public void WriteDataToArrowFile() {
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Schema schema = new Schema(asList(age, name), null);
        try (
                BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                IntVector ageVector = (IntVector) root.getVector("age");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");
        ){
            root.setRowCount(3);
            ageVector.allocateNew(3);
            ageVector.set(0, 10);
            ageVector.set(1, 20);
            ageVector.set(2, 30);

            nameVector.allocateNew(3);
            nameVector.set(0, "Dave".getBytes(StandardCharsets.UTF_8));
            nameVector.set(1, "Peter".getBytes(StandardCharsets.UTF_8));
            nameVector.set(2, "Mary".getBytes(StandardCharsets.UTF_8));

            // Call setRowCount after vectors have been set, otherwise all values become null when the file is read.
            root.setRowCount(3);

            System.out.println("VectorSchemaRoot read: \n" + root.contentToTSVString());
            System.out.println(root.getRowCount());
            File file = new File("random_access_file.arrow");
            try (
                FileOutputStream fileOutputStream = new FileOutputStream(file);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, fileOutputStream.getChannel());
            ){
                writer.start();
                writer.writeBatch();
                writer.end();
                System.out.println("Record batches written: " + writer.getRecordBlocks().size()
                        + ". Number of rows written: " + root.getRowCount());
                Assert.assertEquals(writer.getRecordBlocks().size(), 1);
                Assert.assertEquals(root.getRowCount(), 3);
            } catch (IOException e){
                e.printStackTrace();
            }
        }

        File file = new File("random_access_file.arrow");
        try (
            BufferAllocator allocator = new RootAllocator();
            FileInputStream fileInputStream = new FileInputStream(file);
            ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), allocator);
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock: reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                System.out.println("Number of rows read: " + root.getRowCount());
                System.out.println("VectorSchemaRoot read: \n" + root.contentToTSVString());
                System.out.println(root.getSchema());
                System.out.println(root.getVector("age"));
                Assert.assertEquals(root.contentToTSVString(),
                        "age\tname\n" +
                                "10\tDave\n" +
                                "20\tPeter\n" +
                                "30\tMary\n");
            }
            if(file.delete()){
                System.out.println(file.getName() + " 文件已被删除！");
            }else{
                System.out.println("文件删除失败！");
            }

        } catch (IOException e){
            e.printStackTrace();
        }
    }


}
