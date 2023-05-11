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
 *
 */

package org.apache.flink.lakesoul.connector;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.PartitionReader;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LakeSoulPartitionReader implements PartitionReader<LakeSoulPartition, RowData> {

    private NativeIOReader nativeIOReader;
    private transient LakeSoulArrowReader lakesoulArrowReader;
    private final List<String> filePathList;
    private final List<String> primaryKeys;
    private final RowType schema;
    private final int capacity;
    private final int threadNum;
    private final Configuration conf;
    private final int awaitTimeout;
    private transient VectorSchemaRoot currentVSR;
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
        this.curPartitionId = -1;
    }
    /**
     * Opens the reader with given partitions.
     *
     * @param partitions
     */
    @Override
    public void open(List<LakeSoulPartition> partitions) throws IOException {
        this.partitions = partitions;
        this.curPartitionId = -1;
        this.currentVSR = null;
        this.curArrowReader = null;
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
        Optional<RowData> rowData = nextRecord();
        return rowData.orElse(null);
    }

    private Optional<RowData> nextRecord() throws IOException {
         if (curArrowReader == null || curRecordId >= currentVSR.getRowCount()) {
            curArrowReader = nextBatch();
        }
        if (curArrowReader == null) return Optional.empty();
        RowData rd = curArrowReader.read(curRecordId);
        curRecordId++;
        return Optional.of(rd);
    }

    private ArrowReader nextBatch() throws IOException {
        while (lakesoulArrowReader == null || !lakesoulArrowReader.hasNext()) {
            curPartitionId++;
            if (curPartitionId >= partitions.size()) return null;
            recreateInnerReaderForSinglePartition(curPartitionId);
        }
        if (lakesoulArrowReader == null) return null;

        currentVSR = lakesoulArrowReader.nextResultVectorSchemaRoot();
        if (this.currentVSR == null) {
            throw new IOException("nextVectorSchemaRoot not ready");
        }
        curRecordId = 0;
        return ArrowUtils.createArrowReader(currentVSR, this.schema);
    }

    private void recreateInnerReaderForSinglePartition(int partitionIndex) throws IOException {
        if (partitionIndex >= partitions.size()) {
            lakesoulArrowReader = null;
            return;
        }
        nativeIOReader = new NativeIOReader();
        LakeSoulPartition partition = partitions.get(partitionIndex);
        for (Path path: partition.getPaths()) {
            nativeIOReader.addFile(path.getPath());
        }

        for (int i = 0; i < partition.getPartitionKeys().size(); i++) {
            nativeIOReader.setDefaultColumnValue(partition.getPartitionKeys().get(i), partition.getPartitionValues().get(i));
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
    }

    /**
     * Close the reader, this method should release all resources.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     */
    @Override
    public void close() throws IOException {
        if (lakesoulArrowReader != null) lakesoulArrowReader.close();
        if (currentVSR != null) currentVSR.close();
    }
}
