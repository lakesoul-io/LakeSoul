// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.parquet;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.arrow.ArrowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.NativeIOOptions;
import org.apache.spark.sql.vectorized.NativeIOUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.LongType;


/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 * <p>
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 * <p>
 * This class can either return InternalRows or ColumnarBatches. With whole stage codegen
 * enabled, this class returns ColumnarBatches which offers significant performance gains.
 * TODO: make this always return ColumnarBatches.
 */
public class NativeVectorizedReader extends SpecificParquetRecordReaderBase<Object> {
    // The capacity of vectorized batch.
    private final int capacity;

    /**
     * Batch of rows that we assemble and the current index we've returned. Every time this
     * batch is used up (batchIdx == numBatched), we populated the batch.
     */
    private int batchIdx = 0;

    private int numBatched = 0;

    /**
     * The number of rows that have been returned.
     */
    private long rowsReturned;

    /**
     * The timezone that timestamp INT96 values should be converted to. Null if no conversion. Here to
     * workaround incompatibilities between different engines when writing timestamp values.
     */
    private final ZoneId convertTz;

    /**
     * The mode of rebasing date/timestamp from Julian to Proleptic Gregorian calendar.
     */
    private final String datetimeRebaseMode;

    /**
     * The mode of rebasing INT96 timestamp from Julian to Proleptic Gregorian calendar.
     */
    private final String int96RebaseMode;

    /**
     * columnBatch object that is used for batch decoding. This is created on first use and triggers
     * batched decoding. It is not valid to interleave calls to the batched interface with the row
     * by row RecordReader APIs.
     * This is only enabled with additional flags for development. This is still a work in progress
     * and currently unsupported cases will fail with potentially difficult to diagnose errors.
     * This should be only turned on for development to work on this feature.
     * <p>
     * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
     * code between the path that uses the MR decoders and the vectorized ones.
     */
    private ColumnarBatch columnarBatch;

    private WritableColumnVector[] partitionColumnVectors = null;

    private StructType partitionColumns = null;

    private InternalRow partitionValues = null;

    private ColumnVector[] nativeColumnVector = null;

    /**
     * If true, this class returns batches instead of rows.
     */
    private boolean returnColumnarBatch;

    /**
     * The memory mode of the columnarBatch
     */
    private final MemoryMode MEMORY_MODE;

    public NativeVectorizedReader(
            ZoneId convertTz,
            String datetimeRebaseMode,
            String int96RebaseMode,
            boolean useOffHeap,
            int capacity) {
        this(convertTz, datetimeRebaseMode, int96RebaseMode, useOffHeap, capacity, null);
    }

    public NativeVectorizedReader(
            ZoneId convertTz,
            String datetimeRebaseMode,
            String int96RebaseMode,
            boolean useOffHeap,
            int capacity,
            FilterPredicate filter) {
        this.convertTz = convertTz;
        this.datetimeRebaseMode = datetimeRebaseMode;
        this.int96RebaseMode = int96RebaseMode;
        MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
        this.capacity = capacity;
        this.filter = filter;
    }

    public void initialize(InputSplit[] inputSplits, TaskAttemptContext taskAttemptContext, StructType requestSchema)
            throws IOException, InterruptedException, UnsupportedOperationException {
        assert (inputSplits.length == 1);
        initialize(inputSplits, taskAttemptContext, null, requestSchema, null);
    }

    public void initialize(InputSplit[] inputSplits, TaskAttemptContext taskAttemptContext, String[] primaryKeys, StructType requestSchema, Map<String, String> mergeOperatorInfo)
            throws IOException, InterruptedException, UnsupportedOperationException {
        super.initialize(inputSplits[0], taskAttemptContext);
        FileSplit split = (FileSplit) inputSplits[0];
        this.file = split.getPath();
        this.nativeIOOptions = NativeIOUtils.getNativeIOOptions(taskAttemptContext, this.file);
        this.filePathList = new ArrayList<>();

        for (int i = 0; i < inputSplits.length; i++) {
            FileSplit fileSplit = (FileSplit) inputSplits[i];
            this.filePathList.add(fileSplit.getPath().toString());
        }

        if (primaryKeys != null) {
            this.primaryKeys = Arrays.asList(primaryKeys);
        }
        this.mergeOps = mergeOperatorInfo;
        this.requestSchema = requestSchema == null ? sparkSchema : requestSchema;
        initializeInternal();
        TaskContext.get().addTaskCompletionListener(context -> {
            try {
                close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (columnarBatch != null) {
            columnarBatch.close();
            columnarBatch = null;
        }
        if (nativeReader != null) {
            nativeReader.close();
            nativeReader = null;
        }
        super.close();
    }

    public void closeCurrentBatch() {
        if (nativeColumnVector != null) {
            for (ColumnVector c : nativeColumnVector) {
                c.close();
            }
            nativeColumnVector = null;
        }
        if (columnarBatch != null) {
            columnarBatch = null;
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        return nextBatch();
    }

    @Override
    public Object getCurrentValue() {
        if (returnColumnarBatch) return columnarBatch;
        return columnarBatch.getRow(batchIdx - 1);
    }

    @Override
    public float getProgress() {
        return (float) rowsReturned / totalRowCount;
    }

    public void setAwaitTimeout(int awaitTimeout) {
        this.awaitTimeout = awaitTimeout;
    }

    public void setPrefetchBufferSize(int prefetchBufferSize) {
        this.prefetchBufferSize = prefetchBufferSize;
    }

    public void setThreadNum(int threadNum) {
        this.threadNum = threadNum;
    }

    private void recreateNativeReader() throws IOException {
        close();
        NativeIOReader reader = new NativeIOReader();
        for (String path : filePathList) {
            reader.addFile(path);
        }
        if (primaryKeys != null) {
            reader.setPrimaryKeys(primaryKeys);
        }

        String timeZoneId = convertTz == null ? SQLConf.get().sessionLocalTimeZone() : convertTz.toString();
        Schema arrowSchema = ArrowUtils.toArrowSchema(requestSchema, timeZoneId);
        reader.setSchema(arrowSchema);

        if (partitionColumns != null) {
            for (int i = 0; i < partitionColumns.fields().length; i++) {
                StructField field = partitionColumns.fields()[i];
                reader.setDefaultColumnValue(field.name(), partitionValues.get(i, field.dataType()).toString());
            }
        }
        reader.setBatchSize(capacity);
        reader.setBufferSize(prefetchBufferSize);
        reader.setThreadNum(threadNum);

        NativeIOUtils.setNativeIOOptions(reader, this.nativeIOOptions);

        if (filter != null) {
            reader.addFilter(filterEncode(filter));
        }

        if (mergeOps != null) {
            reader.addMergeOps(mergeOps);
        }

        reader.initializeReader();

        totalRowCount = 0;
        nativeReader = new LakeSoulArrowReader(reader, awaitTimeout);

    }

    private String filterEncode(FilterPredicate filter) {
        return filter.toString();
    }

    // Create partitions' column vector
    private void initBatch(
            MemoryMode memMode,
            StructType partitionColumns,
            InternalRow partitionValues) throws IOException {
        this.partitionColumns = partitionColumns;
        this.partitionValues = partitionValues;
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
            StructType newSchema = new StructType();
            for (StructField f : requestSchema.fields()) {
                boolean is_partition = false;
                for (StructField partitionField : partitionColumns.fields()) {
                    if (partitionField.name().equals(f.name())) is_partition = true;
                }
                if (!is_partition) newSchema = newSchema.add(f);
            }
            for (StructField partitionField : partitionColumns.fields()) {
                newSchema = newSchema.add(partitionField);
            }
            requestSchema = newSchema;
        } else {
            partitionColumns = new StructType(new StructField[]{new StructField("empty row", LongType, false, Metadata.empty())});

            partitionValues = new GenericInternalRow(new Long[]{0L});
            if (partitionColumnVectors != null) {
                for (WritableColumnVector c : partitionColumnVectors) {
                    c.close();
                }
            }
            if (memMode == MemoryMode.OFF_HEAP) {
                partitionColumnVectors = OffHeapColumnVector.allocateColumns(capacity, partitionColumns);
            } else {
                partitionColumnVectors = OnHeapColumnVector.allocateColumns(capacity, partitionColumns);
            }
            for (int i = 0; i < partitionColumns.fields().length; i++) {
                ColumnVectorUtils.populate(partitionColumnVectors[i], partitionValues, i);
                partitionColumnVectors[i].setIsConstant();
            }
        }
        recreateNativeReader();
    }

    private void initBatch() throws IOException {
        initBatch(MEMORY_MODE, null, null);
    }

    public void initBatch(StructType partitionColumns, InternalRow partitionValues) throws IOException {
        initBatch(MEMORY_MODE, partitionColumns, partitionValues);
    }

    /**
     * Can be called before any rows are returned to enable returning columnar batches directly.
     */
    public void enableReturningBatches() {
        returnColumnarBatch = true;
    }

    /**
     * Advances to the next batch of rows. Returns false if there are no more.
     */
    public boolean nextBatch() throws IOException {
        closeCurrentBatch();
        if (nativeReader.hasNext()) {
            VectorSchemaRoot nextVectorSchemaRoot = nativeReader.nextResultVectorSchemaRoot();
            if (nextVectorSchemaRoot == null) {
                throw new IOException("nextVectorSchemaRoot not ready");
            } else {
                int rowCount = nextVectorSchemaRoot.getRowCount();
                if (nextVectorSchemaRoot.getSchema().getFields().isEmpty()) {
                    if (partitionColumnVectors == null) {
                        throw new IOException("NativeVectorizedReader has not been initialized");
                    }
                    columnarBatch = new ColumnarBatch(partitionColumnVectors, rowCount);
                } else {
                    nativeColumnVector = NativeIOUtils.asArrayColumnVector(nextVectorSchemaRoot);
                    columnarBatch = new ColumnarBatch(nativeColumnVector, rowCount);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private void initializeInternal() throws IOException, UnsupportedOperationException {
        recreateNativeReader();
        initBatch();
    }

    private LakeSoulArrowReader nativeReader = null;

    private StructType requestSchema = null;

    private int prefetchBufferSize = 1;

    private int threadNum = 2;

    private int awaitTimeout = 10000;

    private List<String> filePathList;

    private List<String> primaryKeys = null;

    private NativeIOOptions nativeIOOptions;

    private Map<String, String> mergeOps = null;

    private final FilterPredicate filter;
}
