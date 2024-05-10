package org.apache.flink.lakesoul.test.mock;

import com.dmetasoul.lakesoul.meta.entity.TableInfo;
import com.dmetasoul.lakesoul.meta.entity.TableInfoOrBuilder;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.VarCharWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowTypeInfo;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.lakesoul.test.AbstractTestBase.getTempDirUri;

public class MockLakeSoulArrowSource {


    public static class MockSourceFunction implements SourceFunction<LakeSoulArrowWrapper>, ResultTypeQueryable {

        private final int count;
        private final long interval;

        private final static BufferAllocator allocator = ArrowUtils.getRootAllocator();

        final static String STRUCT_INT_CHILD = "struct_int_child";
        final static String STRUCT_UTF8_CHILD = "struct_utf8_child";

        public static final Schema schema = new Schema(
                Arrays.asList(
                        new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)
//                        new Field("utf8", FieldType.nullable(new ArrowType.Utf8()), null),
//                        new Field("decimal", FieldType.nullable(ArrowType.Decimal.createDecimal(10, 3, null)), null),
//                        new Field("boolean", FieldType.nullable(new ArrowType.Bool()), null),
//                        new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.DAY)), null),
//                        new Field("datetimeSec", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.SECOND, ZoneId.of("UTC").toString())), null),
//                        new Field("datetimeMilli", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, ZoneId.of("UTC").toString())), null),
//                        new Field("list", FieldType.nullable(new ArrowType.List()),
//                                Collections.singletonList(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)))
                )
        );


        public MockSourceFunction(int count, long interval) {
            this.count = count;
            this.interval = interval;
        }

        /**
         * Starts the source. Implementations use the {@link SourceContext} to emit elements. Sources
         * that checkpoint their state for fault tolerance should use the {@link
         * SourceContext#getCheckpointLock()} checkpoint lock} to ensure consistency between the
         * bookkeeping and emitting the elements.
         *
         * <p>Sources that implement {@link CheckpointedFunction} must lock on the {@link
         * SourceContext#getCheckpointLock()} checkpoint lock} checkpoint lock (using a synchronized
         * block) before updating internal state and emitting elements, to make both an atomic
         * operation.
         *
         * <p>Refer to the {@link SourceFunction top-level class docs} for an example.
         *
         * @param ctx The context to emit elements to and for accessing locks.
         */
        @Override
        public void run(SourceContext<LakeSoulArrowWrapper> ctx) throws Exception {
            for (int i = 0; i < count; i++) {
                long now = System.currentTimeMillis();
                ctx.collect(new LakeSoulArrowWrapper(mockTableInfo(now), mockVectorSchemaRoot(now)));
                Thread.sleep(interval);
            }

        }

        private VectorSchemaRoot mockVectorSchemaRoot(long now) {

            VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
            int batchSize = 20;
            root.setRowCount(batchSize);
            for (int idx = 0; idx < schema.getFields().size(); idx++) {
                setValue(allocator, root, root.getVector(idx), idx, batchSize);
            }

            return root;
        }

        private static void setValue(BufferAllocator allocator, VectorSchemaRoot root, FieldVector fieldVector, int columnIdx, int batchSize) {
            if (fieldVector instanceof TinyIntVector) {
                TinyIntVector vector = (TinyIntVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof SmallIntVector) {
                SmallIntVector vector = (SmallIntVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof IntVector) {
                IntVector vector = (IntVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof BigIntVector) {
                BigIntVector vector = (BigIntVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7L + i * 3L);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof BitVector) {
                BitVector vector = (BitVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, (columnIdx * 7 + i * 3) & 1);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof Float4Vector) {
                Float4Vector vector = (Float4Vector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof Float8Vector) {
                Float8Vector vector = (Float8Vector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof VarCharVector) {
                VarCharVector vector = (VarCharVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, new Text(String.valueOf(columnIdx * 101 + i * 3)));
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof FixedSizeBinaryVector) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));
            } else if (fieldVector instanceof VarBinaryVector) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));
            } else if (fieldVector instanceof DecimalVector) {
                DecimalVector vector = (DecimalVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7L + i * 3L);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);

            } else if (fieldVector instanceof DateDayVector) {
                DateDayVector vector = (DateDayVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7 + i * 3);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof DateMilliVector) {
                DateMilliVector vector = (DateMilliVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7L + i * 3L);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);
            } else if (fieldVector instanceof TimeSecVector
                    || fieldVector instanceof TimeMilliVector
                    || fieldVector instanceof TimeMicroVector
                    || fieldVector instanceof TimeNanoVector) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));
            } else if (fieldVector instanceof TimeStampVector) {
                TimeStampVector vector = (TimeStampVector) fieldVector;
                vector.allocateNew(batchSize);
                for (int i = 0; i < batchSize; i++) {
                    vector.set(i, columnIdx * 7L + i * 3L);
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }
                vector.setValueCount(batchSize);

            } else if (fieldVector instanceof MapVector) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));

            } else if (fieldVector instanceof ListVector) {
                ListVector vector = (ListVector) fieldVector;
                vector.allocateNew();
                UnionListWriter writer = vector.getWriter();
                int count = 0;
                for (int i = 0; i < batchSize; i++) {
                    writer.startList();
                    int subCount = (columnIdx * 7 + i * 3) % 5;
                    writer.setPosition(i);
                    for (int j = 0; j < subCount; j++) {
                        writer.writeInt(columnIdx * 7 + i * 3 + j * 11);
                    }
                    writer.setValueCount(subCount);
                    count += subCount;

                    writer.endList();
                    if ((i + columnIdx) % 5 == 0) {
                        vector.setNull(i);
                    }
                }

                vector.setValueCount(count);
            } else if (fieldVector instanceof StructVector) {
                StructVector vector = (StructVector) fieldVector;
                NullableStructWriter writer = vector.getWriter();
                IntWriter intWriter = writer.integer(STRUCT_INT_CHILD);
                VarCharWriter varCharWriter = writer.varChar(STRUCT_UTF8_CHILD);
                for (int i = 0; i < batchSize; i++) {
                    writer.setPosition(i);
                    writer.start();
                    intWriter.writeInt(columnIdx * 7 + i * 3);

                    byte[] bytes = new Text(String.valueOf(columnIdx * 101 + i * 3)).getBytes();
                    ArrowBuf buf = allocator.buffer(bytes.length);
                    buf.writeBytes(bytes);
                    varCharWriter.writeVarChar(0, bytes.length, buf);
                    buf.close();
                    writer.end();
                }

                writer.setValueCount(batchSize);
            } else if (fieldVector instanceof NullVector) {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));
            } else {
                throw new UnsupportedOperationException(
                        String.format("Unsupported type %s.", fieldVector.getField()));
            }
        }


        private TableInfo mockTableInfo(long now) {
            return TableInfo.newBuilder().build();
        }

        /**
         * Cancels the source. Most sources will have a while loop inside the {@link
         * #run(SourceContext)} method. The implementation needs to ensure that the source will break
         * out of that loop after this method is called.
         *
         * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
         * {@code false} in this method. That flag is checked in the loop condition.
         *
         * <p>In case of an ungraceful shutdown (cancellation of the source operator, possibly for
         * failover), the thread that calls {@link #run(SourceContext)} will also be {@link
         * Thread#interrupt() interrupted}) by the Flink runtime, in order to speed up the cancellation
         * (to ensure threads exit blocking methods fast, like I/O, blocking queues, etc.). The
         * interruption happens strictly after this method has been called, so any interruption handler
         * can rely on the fact that this method has completed (for example to ignore exceptions that
         * happen after cancellation).
         *
         * <p>During graceful shutdown (for example stopping a job with a savepoint), the program must
         * cleanly exit the {@link #run(SourceContext)} method soon after this method was called. The
         * Flink runtime will NOT interrupt the source thread during graceful shutdown. Source
         * implementors must ensure that no thread interruption happens on any thread that emits records
         * through the {@code SourceContext} from the {@link #run(SourceContext)} method; otherwise the
         * clean shutdown may fail when threads are interrupted while processing the final records.
         *
         * <p>Because the {@code SourceFunction} cannot easily differentiate whether the shutdown should
         * be graceful or ungraceful, we recommend that implementors refrain from interrupting any
         * threads that interact with the {@code SourceContext} at all. You can rely on the Flink
         * runtime to interrupt the source thread in case of ungraceful cancellation. Any additionally
         * spawned threads that directly emit records through the {@code SourceContext} should use a
         * shutdown method that does not rely on thread interruption.
         */
        @Override
        public void cancel() {
        }

        /**
         * Gets the data type (as a {@link TypeInformation}) produced by this function or input format.
         *
         * @return The data type produced by this function or input format.
         */
        @Override
        public TypeInformation getProducedType() {
            System.out.println("[debug]getProducedType");
            return new LakeSoulArrowTypeInfo(schema);
        }
    }

}
