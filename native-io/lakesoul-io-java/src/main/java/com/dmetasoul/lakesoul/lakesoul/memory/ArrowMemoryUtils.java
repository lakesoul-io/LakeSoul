// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.lakesoul.memory;

import com.dmetasoul.lakesoul.lakesoul.GlobalResourceManager;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class ArrowMemoryUtils {
    public final static BufferAllocator rootAllocator = new RootAllocator();

    static {
        GlobalResourceManager.register(rootAllocator);
    }
}
