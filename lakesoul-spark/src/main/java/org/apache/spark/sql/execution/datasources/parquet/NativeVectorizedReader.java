/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet;

import org.apache.arrow.lakesoul.io.NativeIOReader;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.Type;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.NativeIOUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;


/**
 * A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the
 * Parquet column APIs. This is somewhat based on parquet-mr's ColumnReader.
 *
 * TODO: handle complex types, decimal requiring more than 8 bytes, INT96. Schema mismatch.
 * All of these can be handled efficiently and easily with codegen.
 *
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
   * For each column, true if the column is missing in the file and we'll instead return NULLs.
   */
  private boolean[] missingColumns;

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
   *
   * When this is set, the code will branch early on in the RecordReader APIs. There is no shared
   * code between the path that uses the MR decoders and the vectorized ones.
   */
  private ColumnarBatch columnarBatch;

  private WritableColumnVector[] partitionColumnVectors;

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
          FilterPredicate filter)
  {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.int96RebaseMode = int96RebaseMode;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    this.capacity = capacity;
    this.filter = filter;
  }

  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
    FileSplit split = (FileSplit) inputSplit;
    this.file = split.getPath();
    this.filePath = file.toString();
    FileSystem fileSystem = file.getFileSystem(taskAttemptContext.getConfiguration());
    if (fileSystem instanceof S3AFileSystem) {
      s3aFileSystem = (S3AFileSystem) fileSystem;
      awsS3Bucket = s3aFileSystem.getBucket();
      s3aEndpoint = taskAttemptContext.getConfiguration().get("fs.s3a.endpoint");
      s3aRegion = taskAttemptContext.getConfiguration().get("fs.s3a.endpoint.region");
      s3aAccessKey = taskAttemptContext.getConfiguration().get("fs.s3a.access.key");
      s3aSecretKey = taskAttemptContext.getConfiguration().get("fs.s3a.secret.key");
    }
    initializeInternal();
  }

  /**
   * Utility API that will read all the data in path. This circumvents the need to create Hadoop
   * objects to use this class. `columns` can contain the list of columns to project.
   */
  @Override
  public void initialize(String path, List<String> columns) throws IOException,
          UnsupportedOperationException {
    super.initialize(path, columns);
    this.filePath = path;
    initializeInternal();
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

  @Override
  public boolean nextKeyValue() throws IOException {
    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
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
    if (nativeReader != null) {
      nativeReader.close();
      nativeReader = null;
    }
    NativeIOReader reader = new NativeIOReader();
    reader.addFile(filePath);
    // Initialize missing columns with nulls.
    for (int i = 0; i < missingColumns.length; i++) {
      if (!missingColumns[i]) {
        reader.addColumn(sparkSchema.fields()[i].name());
      }
    }

    Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, convertTz == null ? "" : convertTz.toString());
    reader.setSchema(arrowSchema);

    reader.setBatchSize(capacity);
    reader.setBufferSize(prefetchBufferSize);
    reader.setThreadNum(threadNum);

    if (s3aFileSystem != null) {
      reader.setObjectStoreOptions(s3aAccessKey, s3aSecretKey, s3aRegion, awsS3Bucket, s3aEndpoint);
    }

    if (filter != null) {
      reader.addFilter(filterEncode(filter));
    }

    reader.initializeReader();

    totalRowCount= 0;
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
    recreateNativeReader();

    if (partitionColumns != null) {
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
  }

  private void initBatch() throws IOException {
    initBatch(MEMORY_MODE, null, null);
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) throws IOException {
    initBatch(MEMORY_MODE, partitionColumns, partitionValues);
  }

  /**
   * Returns the ColumnarBatch object that will be used for all rows returned by this reader.
   * This object is reused. Calling this enables the vectorized reader. This should be called
   * before any calls to nextKeyValue/nextBatch.
   */
  public ColumnarBatch resultBatch() throws IOException {
    if (columnarBatch == null) initBatch();
    return columnarBatch;
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
    if (nativeReader.hasNext()) {
      VectorSchemaRoot nextVectorSchemaRoot = nativeReader.nextResultVectorSchemaRoot();
      if (nextVectorSchemaRoot == null) {
        throw new IOException("nextVectorSchemaRoot not ready");
      } else {
        totalRowCount += nextVectorSchemaRoot.getRowCount();
        ColumnVector[] nativeColumnVector = NativeIOUtils.asArrayColumnVector(nextVectorSchemaRoot);
        ColumnVector[] resultColumnVector = Arrays.copyOf(nativeColumnVector, nativeColumnVector.length + partitionColumnVectors.length);
        System.arraycopy(partitionColumnVectors, 0, resultColumnVector, nativeColumnVector.length, partitionColumnVectors.length);

        columnarBatch = new ColumnarBatch(resultColumnVector, nextVectorSchemaRoot.getRowCount());
      }
      return true;
    } else {
      return false;
    }
  }

  private void initializeInternal() throws IOException, UnsupportedOperationException {
    // Check that the requested schema is supported.
    missingColumns = new boolean[requestedSchema.getFieldCount()];
    List<ColumnDescriptor> columns = requestedSchema.getColumns();
    List<String[]> paths = requestedSchema.getPaths();
    for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
      Type t = requestedSchema.getFields().get(i);

      if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
        throw new UnsupportedOperationException("Complex types not supported.");
      }

      String[] colPath = paths.get(i);
      if (fileSchema.containsPath(colPath)) {
        ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
        if (!fd.equals(columns.get(i))) {
          throw new UnsupportedOperationException("Schema evolution not supported.");
        }
        missingColumns[i] = false;
      } else {
        if (columns.get(i).getMaxDefinitionLevel() == 0) {
          // Column is missing in data but the required data is non-nullable. This file is invalid.
          throw new IOException("Required column is missing in data file. Col: " +
                  Arrays.toString(colPath));
        }
        missingColumns[i] = true;
      }
    }

    //initbatch with empty partition column
    initBatch();
  }

  private LakeSoulArrowReader nativeReader = null;

  private int prefetchBufferSize = 2;

  private int threadNum = 2;

  private int awaitTimeout = 10000;

  private String filePath;

  private S3AFileSystem s3aFileSystem = null;
  private String s3aEndpoint = null;
  private String s3aRegion = null;
  private String s3aAccessKey = null;
  private String s3aSecretKey = null;

  private String awsS3Bucket = null;

  private final FilterPredicate filter;
}


