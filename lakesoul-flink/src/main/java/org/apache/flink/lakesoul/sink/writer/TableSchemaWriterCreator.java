/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.sink.bucket.CdcPartitionComputer;
import org.apache.flink.lakesoul.sink.bucket.FlinkBucketAssigner;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
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
        creator.identity = new TableSchemaIdentity(tableId, rowType, tableLocation, primaryKeys, partitionKeyList, FlinkUtil.getPropertiesFromConfiguration(conf));
        creator.primaryKeys = primaryKeys;
        creator.partitionKeyList = partitionKeyList;
        creator.outputFileConfig = OutputFileConfig.builder().build();

        creator.partitionComputer = new CdcPartitionComputer(
                "null",
                rowType.getFieldNames().toArray(new String[0]),
                rowType,
                partitionKeyList.toArray(new String[0]),
                conf.getBoolean(LakeSoulSinkOptions.USE_CDC)
        );

        creator.bucketAssigner = new FlinkBucketAssigner(creator.partitionComputer);
        creator.tableLocation = FlinkUtil.makeQualifiedPath(tableLocation);
        return creator;
    }

    public BucketWriter<RowData, String> createBucketWriter() throws IOException {
        if (NativeIOBase.isNativeIOLibExist()) {
            LOG.info("Create natvie bucket writer");
            return new NativeBucketWriter(this.identity.rowType, this.primaryKeys, this.conf);
        } else {
            String msg = "Cannot load lakesoul native writer";
            LOG.error(msg);
            throw new IOException(msg);
        }
    }
}
