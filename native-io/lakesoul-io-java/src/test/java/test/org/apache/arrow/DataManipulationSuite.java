package test.org.apache.arrow;

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.junit.Assert;
import org.junit.Test;

public class DataManipulationSuite {

    @Test
    public void CompareVectorsForFieldEquality() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector right = new IntVector("int", allocator);
        ) {
            right.allocateNew(3);
            right.set(0, 10);
            right.set(1, 20);
            right.set(2, 30);
            right.setValueCount(3);
            IntVector left1 = new IntVector("int", allocator);
            IntVector left2 = new IntVector("int2", allocator);
            TypeEqualsVisitor visitor = new TypeEqualsVisitor(right);
            System.out.println(visitor.equals(left1));
            Assert.assertTrue(visitor.equals(left1));
            System.out.println(visitor.equals(left2));
            Assert.assertFalse(visitor.equals(left2));
        }
    }

    @Test
    public void CompareVectorEquality() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector vector1 = new IntVector("vector1", allocator);
                IntVector vector2 = new IntVector("vector1", allocator);
                IntVector vector3 = new IntVector("vector1", allocator);
        ) {
            vector1.allocateNew(1);
            vector1.set(0, 10);
            vector1.setValueCount(1);

            vector2.allocateNew(1);
            vector2.set(0, 10);
            vector2.setValueCount(1);

            vector3.allocateNew(1);
            vector3.set(0, 20);
            vector3.setValueCount(1);

            System.out.println(VectorEqualsVisitor.vectorEquals(vector1, vector2));
            System.out.println(VectorEqualsVisitor.vectorEquals(vector1, vector3));
            Assert.assertTrue(VectorEqualsVisitor.vectorEquals(vector1, vector2));
            Assert.assertFalse(VectorEqualsVisitor.vectorEquals(vector1, vector3));
        }
    }

    @Test
    public void CompareValuesOnArray() {
        try (
                BufferAllocator allocator = new RootAllocator();
                VarCharVector vec = new VarCharVector("valueindexcomparator", allocator);
        ) {
            vec.allocateNew(3);
            vec.set(0, "ba".getBytes());
            vec.set(1, "abc".getBytes());
            vec.set(2, "aa".getBytes());
            VectorValueComparator<VarCharVector> valueComparator = DefaultVectorComparators.createDefaultComparator(vec);
            valueComparator.attachVector(vec);

            System.out.println(valueComparator.compare(0, 1) > 0);
            System.out.println(valueComparator.compare(1, 2) < 0);
            Assert.assertTrue(valueComparator.compare(0, 1) > 0);
            Assert.assertFalse(valueComparator.compare(1, 2) < 0);
        }
    }

    @Test
    public void LinearSearchValueOnArray() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector linearSearchVector = new IntVector("linearSearchVector", allocator);
        ) {
            linearSearchVector.allocateNew(10);
            linearSearchVector.setValueCount(10);
            for (int i = 0; i < 10; i++) {
                linearSearchVector.set(i, i);
            }
            VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(linearSearchVector);
            int result = VectorSearcher.linearSearch(linearSearchVector, comparatorInt, linearSearchVector, 3);
            Assert.assertEquals(result, 3);
        }
    }

    @Test
    public void BinarySearchValueOnArray() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector linearSearchVector = new IntVector("linearSearchVector", allocator);
        ) {
            linearSearchVector.allocateNew(10);
            linearSearchVector.setValueCount(10);
            for (int i = 0; i < 10; i++) {
                linearSearchVector.set(i, i);
            }
            VectorValueComparator<IntVector> comparatorInt = DefaultVectorComparators.createDefaultComparator(linearSearchVector);
            int result = VectorSearcher.binarySearch(linearSearchVector, comparatorInt, linearSearchVector, 3);
            Assert.assertEquals(result, 3);
        }
    }

    @Test
    public void InPlaceSorter() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector intVectorNotSorted = new IntVector("intvectornotsorted", allocator);
        ) {
            intVectorNotSorted.allocateNew(3);
            intVectorNotSorted.setValueCount(3);
            intVectorNotSorted.set(0, 10);
            intVectorNotSorted.set(1, 8);
            intVectorNotSorted.setNull(2);
            FixedWidthInPlaceVectorSorter<IntVector> sorter = new FixedWidthInPlaceVectorSorter<IntVector>();
            VectorValueComparator<IntVector> comparator = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
            sorter.sortInPlace(intVectorNotSorted, comparator);

            System.out.println(intVectorNotSorted);
            Assert.assertEquals(intVectorNotSorted.toString(), "[null, 8, 10]");
        }
    }

    @Test
    public void OutPlaceSorter() {
        try (
                BufferAllocator allocator = new RootAllocator();
                IntVector intVectorNotSorted = new IntVector("intvectornotsorted", allocator);
                IntVector intVectorSorted = (IntVector) intVectorNotSorted.getField().getFieldType().createNewSingleVector("new-out-of-place-sorter", allocator, null)
        ) {
            intVectorNotSorted.allocateNew(3);
            intVectorNotSorted.setValueCount(3);
            intVectorNotSorted.set(0, 10);
            intVectorNotSorted.set(1, 8);
            intVectorNotSorted.setNull(2);
            OutOfPlaceVectorSorter<IntVector> sorterOutOfPlaceSorter = new FixedWidthOutOfPlaceVectorSorter<IntVector>();
            VectorValueComparator<IntVector> comparatorOutOfPlaceSorter = DefaultVectorComparators.createDefaultComparator(intVectorNotSorted);
            intVectorSorted.allocateNew(intVectorNotSorted.getValueCount());
            intVectorSorted.setValueCount(intVectorNotSorted.getValueCount());
            sorterOutOfPlaceSorter.sortOutOfPlace(intVectorNotSorted, intVectorSorted, comparatorOutOfPlaceSorter);
            System.out.println(intVectorSorted);
            Assert.assertEquals(intVectorSorted.toString(), "[null, 8, 10]");
        }
    }
}
