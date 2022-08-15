package org.apache.spark.sql.execution.datasources.v2.parquet.Native;

import org.apache.arrow.lakesoul.io.ArrowCDataWrapper;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.schema.Type;
import org.apache.spark.internal.Logging;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.apache.spark.sql.execution.datasources.parquet.SpecificParquetRecordReaderBase;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader;
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function0;

import java.io.Closeable;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

public class NativeMergeVectorizedReader extends SpecificParquetRecordReaderBase<Object> {
  // The capacity of vectorized batch.
  private int capacity;

  /**
   * Batch of rows that we assemble and the current index we've returned. Every time this
   * batch is used up (batchIdx == numBatched), we populated the batch.
   */
  private int batchIdx = 0;
  private int numBatched = 0;

  /**
   * For each request column, the reader to read this column. This is NULL if this column
   * is missing from the file, in which case we populate the attribute with NULL.
   */
  private VectorizedColumnReader[] columnReaders;

  /**
   * The number of rows that have been returned.
   */
  private long rowsReturned;

  /**
   * The number of rows that have been reading, including the current in flight row group.
   */
  private long totalCountLoadedSoFar = 0;

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
   *
   * TODOs:
   *  - Implement v2 page formats (just make sure we create the correct decoders).
   */
  private ColumnarBatch columnarBatch;

  private WritableColumnVector[] columnVectors;

  /**
   * If true, this class returns batches instead of rows.
   */
  private boolean returnColumnarBatch;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public NativeMergeVectorizedReader(
          ZoneId convertTz,
          String datetimeRebaseMode,
          String int96RebaseMode,
          boolean useOffHeap,
          int capacity,
          MergePartitionedFile file) {
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.int96RebaseMode = int96RebaseMode;
    MEMORY_MODE = useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    System.out.println("[Debug][huazeng]on NativeVectorizedReader, capacity:"+capacity);
    this.capacity = capacity;
    this.file = file;
  }


  /**
   * Implementation of RecordReader API.
   */
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException, UnsupportedOperationException {
    super.initialize(inputSplit, taskAttemptContext);
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
//    resultBatch();

    if (returnColumnarBatch) return nextBatch();

    if (batchIdx >= numBatched) {
      if (!nextBatch()) return false;
    }
    ++batchIdx;
    return true;
  }

  @Override
  public Object getCurrentValue() {
//    System.out.println("[Debug][huazeng]on getCurrentValue, rowsReturned:"+rowsReturned + " totalRowCount:" + totalRowCount);
    System.out.println("[Debug][huazeng]on getCurrentValue, columnarBatch.numCols():"+columnarBatch.numCols() + "columnarBatch.numRows():"+columnarBatch.numRows());
    if (returnColumnarBatch) return columnarBatch;
    return columnarBatch.getRow(batchIdx - 1);
//    if (returnColumnarBatch) return columnarBatch;
//    return columnarBatch.getRow(batchIdx - 1);
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  // Creates a columnar batch that includes the schema from the data files and the additional
  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  private void initBatch(
          MemoryMode memMode,
          StructType partitionColumns,
          InternalRow partitionValues) throws IOException {

    System.out.println("[Debug][huazeng]on initBatch, partitionValues: "+partitionValues);
    if (partitionColumns != null) {
      StructType partitionSchema = new StructType();
      for (StructField f : partitionColumns.fields()) {
        partitionSchema = partitionSchema.add(f);
      }

      if (memMode == MemoryMode.OFF_HEAP) {
        partitionColumnVectors = OffHeapColumnVector.allocateColumns((int)totalRowCount, partitionSchema);
      } else {
        partitionColumnVectors = OnHeapColumnVector.allocateColumns((int)totalRowCount, partitionSchema);
      }
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        System.out.println("[Debug][huazeng]on initBatch: partitionColumnVectors.length=" + partitionColumnVectors.length);
        ColumnVectorUtils.populate(partitionColumnVectors[i], partitionValues, i);
        partitionColumnVectors[i].setIsConstant();
      }
    }
    if (nextVectorSchemaRoot == null) {
      System.out.println("nextVectorSchemaRoot not ready");
    } else if (nextVectorSchemaRoot.getRowCount() != totalRowCount) {
      throw new IOException("nextVectorSchemaRoot row count error");
    } else {
      columnarBatch = new ColumnarBatch(concatBatchVectorWithPartitionVectors(ArrowUtils.asArrayColumnVector(nextVectorSchemaRoot)), nextVectorSchemaRoot.getRowCount());
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
      System.out.println("Debug][huazeng]on nextBatch, nativeReader.hasNext()=true");
      nextVectorSchemaRoot = nativeReader.nextResultVectorSchemaRoot();
      initBatch();
//      columnarBatch = new ColumnarBatch(concatBatchVectorWithPartitionVectors(ArrowUtils.asArrayColumnVector(vsr)), vsr.getRowCount());
      return true;
    } else {
      System.out.println("Debug][huazeng]on nextBatch, nativeReader.hasNext()=false");
      return false;
    }
//    columnarBatch.setNumRows(0);
//    if (rowsReturned >= totalRowCount) return false;
////    checkEndOfRowGroup();
//    int num = (int) Math.min((long) capacity, totalRowCount - rowsReturned);
//    rowsReturned += num;
//    columnarBatch.setNumRows((int)totalRowCount);
//    numBatched = num;
//    batchIdx = 0;
//    return true;
  }

  private ColumnVector[] concatBatchVectorWithPartitionVectors(ColumnVector[] batchVectors){
    ColumnVector[] descColumnVectors = new ColumnVector[batchVectors.length + partitionColumnVectors.length];
    System.arraycopy(batchVectors, 0, descColumnVectors, 0, batchVectors.length);
    System.arraycopy(partitionColumnVectors, 0, descColumnVectors, batchVectors.length,  partitionColumnVectors.length);
    System.out.println("[Debug][huazeng]on concatBatchVectorWithPartitionVectors");
    System.out.println(descColumnVectors[batchVectors.length]);
    return descColumnVectors;
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
    // initalizing native reader
    wrapper = new ArrowCDataWrapper();
    wrapper.initializeConfigBuilder();
    wrapper.addFile(file.filePath());
    // Initialize missing columns with nulls.
    for (int i = 0; i < missingColumns.length; i++) {
      if (!missingColumns[i]) {
        wrapper.addColumn(sparkSchema.fields()[i].name());
      }
    }

    wrapper.setThreadNum(1);
    wrapper.createReader();
    wrapper.startReader(bool -> {});
    nativeReader = new LakeSoulArrowReader(wrapper, 1000);
  }

  private void checkEndOfRowGroup() throws IOException {

    if (rowsReturned != totalCountLoadedSoFar) return;
    PageReadStore pages = reader.readNextRowGroup();
    if (pages == null) {
      throw new IOException("expecting more rows but reached last block. Read "
              + rowsReturned + " out of " + totalRowCount);
    }
//    List<ColumnDescriptor> columns = requestedSchema.getColumns();
//    List<Type> types = requestedSchema.asGroupType().getFields();
//    columnReaders = new VectorizedColumnReader[columns.size()];
//    for (int i = 0; i < columns.size(); ++i) {
//      if (missingColumns[i]) continue;
//      columnReaders[i] = new VectorizedColumnReader(
//              columns.get(i),
//              types.get(i).getOriginalType(),
//              pages.getPageReader(columns.get(i)),
//              convertTz,
//              datetimeRebaseMode,
//              int96RebaseMode);
//    }
    totalCountLoadedSoFar += pages.getRowCount();
  }

  private ArrowCDataWrapper wrapper;
  private LakeSoulArrowReader nativeReader;
  private WritableColumnVector[] partitionColumnVectors;

  private MergePartitionedFile file;

  private VectorSchemaRoot nextVectorSchemaRoot;
}
