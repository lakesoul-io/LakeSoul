package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.spark.sql.types.StructField;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LakeSoulPartitionReader implements PartitionReader<LakeSoulPartition, RowData> {

    private NativeIOReader nativeIOReader;
    private LakeSoulArrowReader lakesoulArrowReader;
    private final List<String> filePathList;
    private final List<String> primaryKeys;
    private final RowType schema;
    private final int capacity;
    private final int threadNum;
    private final Configuration conf;
    private final int awaitTimeout;
    private VectorSchemaRoot currentVSR;
    private ArrowReader curArrowReader;
    private int curRecordId = -1;

    private int curPartitionId;

    private List<LakeSoulPartition> partitions;


    public LakeSoulPartitionReader(RowType schema, List<String> primaryKeys) {
        this.filePathList = null;
        this.primaryKeys = primaryKeys;
        this.schema = schema;
        this.capacity = 1;
        this.threadNum = 2;
        this.conf = null;
        this.awaitTimeout = 10000;
        this.curPartitionId = 0;
    }
    /**
     * Opens the reader with given partitions.
     *
     * @param partitions
     */
    @Override
    public void open(List<LakeSoulPartition> partitions) throws IOException {
        this.partitions = partitions;
        recreateInnerReaderForSinglePartition(0);
    }

    /**
     * Reads the next record from the partitions.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     *
     * @param reuse Object that may be reused.
     * @return Read record.
     */
    @Nullable
    @Override
    public RowData read(RowData reuse) throws IOException {
        if (curPartitionId >= partitions.size()) return null;
        if (currentVSR == null || curRecordId >= currentVSR.getRowCount()) {
            if (this.lakesoulArrowReader.hasNext()) {
                this.currentVSR = this.lakesoulArrowReader.nextResultVectorSchemaRoot();
                this.curArrowReader = ArrowUtils.createArrowReader(currentVSR, this.schema);
                if (this.currentVSR == null) {
                    throw new IOException("nextVectorSchemaRoot not ready");
                }
                curRecordId = 0;
            } else {
                this.lakesoulArrowReader.close();
                curPartitionId++;
                if (curPartitionId == partitions.size()) return null;
                recreateInnerReaderForSinglePartition(curPartitionId);
            }
        }

        RowData rd = this.curArrowReader.read(curRecordId);
        curRecordId++;
        return rd;
    }

    private void recreateInnerReaderForSinglePartition(int partitionIndex) throws IOException {
        if (partitionIndex >= partitions.size()) return;
        nativeIOReader = new NativeIOReader();
        LakeSoulPartition partition = partitions.get(partitionIndex);
        for (Path path: partition.getPaths()) {
            nativeIOReader.addFile(path.getPath());
        }

        for (int i = 0; i < partition.getPartitionKeys().size(); i++) {
            nativeIOReader.setDefaultColumnValue(partition.getPartitionKeys().get(i), partition.getGetPartitionValues().get(i));
        }

        if (primaryKeys != null) {
            nativeIOReader.setPrimaryKeys(primaryKeys);
        }
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema);
        nativeIOReader.setSchema(arrowSchema);
        nativeIOReader.setBatchSize(capacity);
        nativeIOReader.setThreadNum(threadNum);
        nativeIOReader.initializeReader();

        lakesoulArrowReader = new LakeSoulArrowReader(nativeIOReader, awaitTimeout);
        currentVSR = null;
    }

    /**
     * Close the reader, this method should release all resources.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     */
    @Override
    public void close() throws IOException {
        this.lakesoulArrowReader.close();
    }
}
