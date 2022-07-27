package org.apache.arrow.lakesoul.memory;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ArrowMemoryUtils {
    public final static BufferAllocator rootAllocator = new RootAllocator();

}
