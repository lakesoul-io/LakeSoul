package org.apache.spark.sql.arrow;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;

public class DataTypeCastUtils {
    private static volatile boolean ALLOW_PRECISION_LOSS = false;

    private static volatile boolean ALLOW_PRECISION_INCREMENT = true;

    /**
     * Compare two StructType, and check if StructType target can be cast from StructType source
     *
     * @param source
     * @param target
     * @return false, if StructType source is equal to StructType target,
     * true, if two StructType is not equal and StructType target can be cast from StructType source
     * @throws IOException if StructType target can not be cast from StructType source
     */
    public static boolean checkSchemaEqualOrCanCast(StructType source, StructType target) throws IOException {

        return false;
    }
}
