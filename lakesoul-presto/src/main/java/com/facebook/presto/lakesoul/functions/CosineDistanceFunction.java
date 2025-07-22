// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.facebook.presto.lakesoul.functions;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.*;

@ScalarFunction("cosine_distance")
@Description("cosine distance of two array with same length")
public class CosineDistanceFunction {
    @SqlType(StandardTypes.DOUBLE)
    @TypeParameter("T")
    public static double consineDistance(
            @TypeParameter("T") Type elementType,
            @SqlType("array(T)") Block x,
            @SqlType("array(T)") Block y)
    {
        int positionCount = x.getPositionCount();
        if (y.getPositionCount() != positionCount) {
            throw new IllegalArgumentException("cosine distance input arrays should have same length");
        }

        if (elementType instanceof RealType) {
            return consineDistanceReal(elementType, x, y, positionCount);
        } else if (elementType instanceof DoubleType) {
            return consineDistanceDouble(elementType, x, y, positionCount);
        }
        throw new IllegalArgumentException("unknown element type for cosine_distance: " + elementType);
    }

    public static double consineDistanceDouble(
            Type elementType,
            Block x,
            Block y,
            int positionCount)
    {
        double x_norm = 0.0;
        double y_norm = 0.0;
        double dot_product = 0.0;
        for (int i = 0; i < positionCount; i++) {
            double x_value = elementType.getDouble(x, i);
            double y_value = elementType.getDouble(y, i);
            x_norm += x_value * x_value;
            y_norm += y_value * y_value;
            dot_product += x_value * y_value;
        }
        return 1.0 - Math.abs(dot_product) / (Math.sqrt(x_norm) * Math.sqrt(y_norm));
    }

    public static double consineDistanceReal(
            Type elementType,
            Block x,
            Block y,
            int positionCount)
    {
        double x_norm = 0.0;
        double y_norm = 0.0;
        double dot_product = 0.0;
        for (int i = 0; i < positionCount; i++) {
            double x_value = Float.intBitsToFloat(x.getInt(i));
            double y_value = Float.intBitsToFloat(y.getInt(i));
            x_norm += x_value * x_value;
            y_norm += y_value * y_value;
            dot_product += x_value * y_value;
        }
        return 1.0 - Math.abs(dot_product) / (Math.sqrt(x_norm) * Math.sqrt(y_norm));
    }
}
