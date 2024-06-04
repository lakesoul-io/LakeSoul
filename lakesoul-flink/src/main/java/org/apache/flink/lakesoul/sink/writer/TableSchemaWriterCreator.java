// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.bucket.CdcPartitionComputer;
import org.apache.flink.lakesoul.sink.bucket.FlinkBucketAssigner;
import org.apache.flink.lakesoul.sink.writer.arrow.NativeArrowBucketWriter;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.lakesoul.types.arrow.LakeSoulArrowWrapper;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NULL_STRING;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.*;

public class TableSchemaWriterCreator implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TableSchemaWriterCreator.class);

    public TableSchemaIdentity identity;

    public List<String> primaryKeys;

    public List<String> partitionKeyList;

    public OutputFileConfig outputFileConfig;

    public CdcPartitionComputer partitionComputer;

    public BucketAssigner<RowData, String> bucketAssigner;

    public Path tableLocation;

    public Configuration conf;

    public static TableSchemaWriterCreator create(
            TableId tableId,
            RowType rowType,
            String tableLocation,
            List<String> primaryKeys,
            List<String> partitionKeyList,
            Configuration conf) throws IOException {
        TableSchemaWriterCreator creator = new TableSchemaWriterCreator();
        creator.conf = conf;
        creator.identity =
                new TableSchemaIdentity(tableId,
                        rowType,
                        tableLocation,
                        primaryKeys,
                        partitionKeyList,
                        conf.getBoolean(USE_CDC, false),
                        conf.getString(CDC_CHANGE_COLUMN, CDC_CHANGE_COLUMN_DEFAULT));
        creator.primaryKeys = primaryKeys;
        creator.partitionKeyList = partitionKeyList;
        creator.outputFileConfig = OutputFileConfig.builder().build();

        creator.partitionComputer = new CdcPartitionComputer(
                LAKESOUL_NULL_STRING,
                rowType.getFieldNames().toArray(new String[0]),
                rowType,
                partitionKeyList.toArray(new String[0]),
                conf.getBoolean(USE_CDC)
        );

        creator.bucketAssigner = new FlinkBucketAssigner(creator.partitionComputer);
        creator.tableLocation = FlinkUtil.makeQualifiedPath(tableLocation);
        return creator;
    }

    public BucketWriter<RowData, String> createBucketWriter() throws IOException {
        if (NativeIOBase.isNativeIOLibExist()) {
            LOG.info("Create natvie bucket writer");
            return new NativeBucketWriter(this.identity.rowType, this.primaryKeys, this.partitionKeyList, this.conf);
        } else {
            String msg = "Cannot load lakesoul native writer";
            LOG.error(msg);
            throw new IOException(msg);
        }
    }

    public BucketWriter<LakeSoulArrowWrapper, String> createArrowBucketWriter() throws IOException {
        if (NativeIOBase.isNativeIOLibExist()) {
            LOG.info("Create natvie bucket writer");
            return new NativeArrowBucketWriter(this.identity.rowType, this.primaryKeys, this.partitionKeyList, this.conf);
        } else {
            String msg = "Cannot load lakesoul native writer";
            LOG.error(msg);
            throw new IOException(msg);
        }
    }
}
