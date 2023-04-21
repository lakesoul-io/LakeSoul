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


    public LakeSoulPartitionReader(RowType schema, List<String> primaryKeys) {
        this.filePathList = null;
        this.primaryKeys = primaryKeys;
        this.schema = schema;
        this.capacity = 1;
        this.threadNum = 2;
        this.conf = null;
        this.awaitTimeout = 10000;
    }
    /**
     * Opens the reader with given partitions.
     *
     * @param partitions
     */
    @Override
    public void open(List<LakeSoulPartition> partitions) throws IOException {
        nativeIOReader = new NativeIOReader();

        for (Path path : partitions.get(0).getPaths()) {
            nativeIOReader.addFile(path.getPath());
        }
        if (primaryKeys != null) {
            nativeIOReader.setPrimaryKeys(primaryKeys);
        }

        Schema arrowSchema = ArrowUtils.toArrowSchema(schema);
        nativeIOReader.setSchema(arrowSchema);

        nativeIOReader.setBatchSize(capacity);
        nativeIOReader.setThreadNum(threadNum);

//        FlinkUtil.setFSConfigs(conf, nativeIOReader);


//        if (filter != null) {
//            reader.addFilter(filterEncode(filter));
//        }
//
//        if (mergeOps != null) {
//            reader.addMergeOps(mergeOps);
//        }

        nativeIOReader.initializeReader();

        lakesoulArrowReader = new LakeSoulArrowReader(nativeIOReader, awaitTimeout);
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
        if (this.currentVSR == null || curRecordId >= currentVSR.getRowCount()) {
            if (this.lakesoulArrowReader.hasNext()) {
                this.currentVSR = this.lakesoulArrowReader.nextResultVectorSchemaRoot();
                this.curArrowReader = ArrowUtils.createArrowReader(currentVSR, this.schema);
                if (this.currentVSR == null) {
                    throw new IOException("nextVectorSchemaRoot not ready");
                }
                curRecordId = 0;
            } else {
                this.lakesoulArrowReader.close();
                return null;
            }
        }
        RowData rd = this.curArrowReader.read(curRecordId);
        curRecordId++;
        return rd;
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
