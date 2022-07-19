import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.Pointer;


public class TestCDataInterface {

    public static void main(String[] args) {
        try(
                BufferAllocator allocator = new RootAllocator();
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
                ArrowArray arrowArray = ArrowArray.allocateNew(allocator)
        ){
            CDataJavaToCppExample.FillInt64Array(
                    arrowSchema.memoryAddress(), arrowArray.memoryAddress());
            try(
                    BigIntVector bigIntVector = (BigIntVector) Data.importVector(
                            allocator, arrowArray, arrowSchema, null)
            ){
                System.out.println("C++-allocated array: " + bigIntVector);
            }
        }
    }
}