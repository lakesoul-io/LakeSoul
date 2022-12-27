package jnr.ffi;

import jnr.ffi.annotations.Delegate;
import jnr.ffi.types.size_t;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * Hello world!
 *
 */
public class Qsort {

    public static interface Compare {
        @Delegate public int compare(Pointer p1, Pointer p2);
    }

    public static final class IntCompare implements Compare {
        public int compare(Pointer p1, Pointer p2) {
            int i1 = p1.getInt(0);
            int i2 = p2.getInt(0);

            return i1 < i2 ? -1 : i1 > i2 ? 1 : 0;
        }
    }

    public interface LibC {
        public int qsort(int[] data, @size_t int count, @size_t int width, Compare compare);
        public int qsort(Pointer data, @size_t long count, @size_t int width, Compare compare);
        public int qsort(Buffer data, @size_t long count, @size_t int width, Compare compare);
    }

    public static void main(String[] args) {

        LibC libc = LibraryLoader.create(LibC.class).load("c");

        int[] numbers = { 2, 1 };
//        int[] numbers = { 8,5,6,7,9,3, 2, 1 };
        System.out.println("qsort using java int[] array");
        System.out.println("before, numbers[0]=" + numbers[0] + " numbers[1]=" + numbers[1]);
        libc.qsort(numbers, 2, 4, new IntCompare());
        System.out.println("after, numbers[0]=" + numbers[0] + " numbers[1]=" + numbers[1]);
        System.out.append('\n');

        System.out.println("sort using native memory");
        Pointer memory = Memory.allocate(Runtime.getRuntime(libc), 8);
        memory.putInt(0, 4);
        memory.putInt(4, 3); // offset is in bytes
        System.out.println("before, memory[0]=" + memory.getInt(0) + " memory[1]=" + memory.getInt(4));
        libc.qsort(memory, 2, 4, new IntCompare());
        System.out.println("after, memory[0]=" + memory.getInt(0) + " memory[1]=" + memory.getInt(4));

        System.out.append('\n');
        System.out.println("qsort using NIO buffer");
        IntBuffer intBuffer = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder()).asIntBuffer();
        intBuffer.put(0, 6);
        intBuffer.put(1, 5); // offset is in units of int elements

        System.out.println("before, buffer[0]=" + intBuffer.get(0) + " buffer[1]=" + intBuffer.get(1));
        libc.qsort(intBuffer, 2, 4, new IntCompare());
        System.out.println("after, buffer[0]=" + intBuffer.get(0) + " buffer[1]=" + intBuffer.get(1));

    }
}
