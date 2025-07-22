// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.functions;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.*;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

@ScalarFunction("hamming_distance_filter")
@Description("filter two arrays that have at least one pair whose hamming distance is smaller than specified value")
public class HammingDistanceMatchFunction {
    @SqlType(StandardTypes.BOOLEAN)
    @TypeParameter("T")
    public static boolean hammingDistanceFilter(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block x,
            @SqlType("array(T)") Block y,
            @SqlType("integer") long value)
    {
        if (!(elementType instanceof BigintType)) {
            throw new IllegalArgumentException("hamming_distance_filter only supports bigint array, but got " + elementType);
        }
        int positionCount = x.getPositionCount();
        if (y.getPositionCount() != positionCount) {
            throw new IllegalArgumentException("cosine distance input arrays should have same length");
        }
        for (int i = 0; i < positionCount; i++) {
            long left = x.getLong(i);
            long right = y.getLong(i);
            if (Long.bitCount(left ^ right) <= value) {
                return true;
            }
        }
        return false;
    }
}
