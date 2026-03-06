// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.source;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.table.PartitionReader;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.LakeSoulOptions;
import org.apache.flink.lakesoul.connector.LakeSoulPartition;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.arrow.ArrowReader;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LakeSoulPartitionReader implements PartitionReader<LakeSoulPartition, RowData> {

    private static final long serialVersionUID = 9049145796236544669L;
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulPartitionReader.class);
    private transient LakeSoulArrowReader lakesoulArrowReader;
    private final TableId tableId;
    private final List<String> primaryKeys;
    private final RowType schema;
    private final int capacity;
    private final Configuration conf;
    private final int awaitTimeout;
    private transient VectorSchemaRoot currentVSR;
    private ArrowReader curArrowReader;
    private int curRecordId = -1;

    private int curPartitionId;

    private List<LakeSoulPartition> partitions;
    private final String cdcColumn;
    private RowData.FieldGetter cdcFieldGetter;


    public LakeSoulPartitionReader(Configuration conf, TableId tableId, RowType schema,
                                   List<String> primaryKeys,
                                   String cdcColumn) {
        this.tableId = tableId;
        this.primaryKeys = primaryKeys;
        this.schema = schema;
        this.capacity = conf.getInteger(LakeSoulOptions.LAKESOUL_NATIVE_IO_BATCH_SIZE);
        this.conf = new Configuration(conf);
        this.awaitTimeout = 10000;
        this.curPartitionId = -1;
        this.cdcColumn = cdcColumn;
        if (cdcColumn != null && !cdcColumn.isEmpty()) {
            int cdcField = schema.getFieldIndex(cdcColumn);
            cdcFieldGetter = RowData.createFieldGetter(new VarCharType(), cdcField);
        }
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
        LOG.info("(Re)open for {}", tableId);
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
        if (cdcColumn != null && !cdcColumn.isEmpty() && rd != null) {
            if (FlinkUtil.isCDCDelete((StringData) cdcFieldGetter.getFieldOrNull(rd))) {
                // batch read from cdc table should filter delete rows
                return nextRecord();
            }
        }
        return Optional.of(rd);
    }

    private ArrowReader nextBatch() throws IOException {
        while (lakesoulArrowReader == null || !lakesoulArrowReader.hasNext()) {
            curPartitionId++;
            if (curPartitionId >= partitions.size()) return null;
            LOG.info("{}, Read partition index {} of {}", tableId, curPartitionId, partitions.size());
            recreateInnerReaderForSinglePartition(curPartitionId);
        }
        if (lakesoulArrowReader == null) return null;

        currentVSR = lakesoulArrowReader.nextResultVectorSchemaRoot();
        curRecordId = 0;
        return ArrowUtils.createArrowReader(currentVSR, this.schema);
    }

    private void recreateInnerReaderForSinglePartition(int partitionIndex) throws IOException {
        if (partitionIndex >= partitions.size()) {
            lakesoulArrowReader = null;
            return;
        }
        NativeIOReader nativeIOReader = new NativeIOReader();
        LakeSoulPartition partition = partitions.get(partitionIndex);
        for (Path path : partition.getPaths()) {
            nativeIOReader.addFile(FlinkUtil.makeQualifiedPath(path).toString());
        }

        for (int i = 0; i < partition.getPartitionKeys().size(); i++) {
            nativeIOReader.setDefaultColumnValue(partition.getPartitionKeys().get(i), partition.getPartitionValues().get(i));
        }

        if (primaryKeys != null) {
            nativeIOReader.setPrimaryKeys(primaryKeys);
        }
        Schema arrowSchema = ArrowUtils.toArrowSchema(schema);
        FlinkUtil.setIOConfigs(conf, nativeIOReader);
        nativeIOReader.setSchema(arrowSchema);
        nativeIOReader.setBatchSize(capacity);
        LOG.info("Initializing arrow reader for table {}, partition {}, cdc {}", tableId, partitionIndex, cdcColumn);
        nativeIOReader.initializeReader();

        lakesoulArrowReader = new LakeSoulArrowReader(nativeIOReader, awaitTimeout);
        LOG.info("{} Created partition reader for {}, pk {}, cdc {}", tableId, partition, primaryKeys, cdcColumn);
    }

    /**
     * Close the reader, this method should release all resources.
     *
     * <p>When this method is called, the reader it guaranteed to be opened.
     */
    @Override
    public void close() throws IOException {
        if (lakesoulArrowReader != null) {
            lakesoulArrowReader.close();
            lakesoulArrowReader = null;
        }
        if (currentVSR != null) {
            currentVSR.close();
            currentVSR = null;
        }
        LOG.info("Partition reader closed for {}", tableId);
    }
}
