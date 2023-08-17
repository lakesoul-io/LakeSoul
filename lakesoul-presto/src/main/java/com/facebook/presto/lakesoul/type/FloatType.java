package com.facebook.presto.lakesoul.type;

import com.facebook.presto.common.block.*;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.*;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.intBitsToFloat;


public final class FloatType
        extends AbstractPrimitiveType
        implements FixedWidthType
{
    public static final FloatType FLOAT = new FloatType();

    private FloatType()
    {
        super(parseTypeSignature(StandardTypes.DOUBLE), double.class);
    }

    @Override
    public final int getFixedSize()
    {
        return Float.BYTES;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return intBitsToFloat(block.getInt(position));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        float leftValue = intBitsToFloat(leftBlock.getInt(leftPosition));
        float rightValue = intBitsToFloat(rightBlock.getInt(rightPosition));

        // direct equality is correct here
        // noinspection FloatingPointEquality
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        // convert to canonical NaN if necessary
        return AbstractIntType.hash(floatToIntBits(intBitsToFloat(block.getInt(position))));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        double leftValue = intBitsToFloat(leftBlock.getInt(leftPosition));
        double rightValue = intBitsToFloat(rightBlock.getInt(rightPosition));
        return Double.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeInt(block.getInt(position)).closeEntry();
        }
    }

    @Override
    public double getDouble(Block block, int position)
    {
        return intBitsToFloat(block.getInt(position));
    }

    @Override
    public double getDoubleUnchecked(UncheckedBlock block, int internalPosition)
    {
        return intBitsToFloat(block.getIntUnchecked(internalPosition));
    }



    @Override
    public void writeDouble(BlockBuilder blockBuilder, double value)
    {
        blockBuilder.writeInt(floatToIntBits((float) value)).closeEntry();
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        int maxBlockSizeInBytes;
        if (blockBuilderStatus == null) {
            maxBlockSizeInBytes = PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
        }
        else {
            maxBlockSizeInBytes = blockBuilderStatus.getMaxPageSizeInBytes();
        }
        return new IntArrayBlockBuilder(
                blockBuilderStatus,
                Math.min(expectedEntries, maxBlockSizeInBytes / Double.BYTES));
    }

    @Override
    public final BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, Double.BYTES);
    }

    @Override
    public final BlockBuilder createFixedSizeBlockBuilder(int positionCount)
    {
        return new IntArrayBlockBuilder(null, positionCount);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == FLOAT;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
