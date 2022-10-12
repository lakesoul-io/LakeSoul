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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.lakesoul.sink.bucket.CdcPartitionComputer;
import org.apache.flink.lakesoul.sink.bucket.FlinkBucketAssigner;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulKeyGen;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.lakesoul.types.LakeSoulCDCComparator;
import org.apache.flink.lakesoul.types.TableId;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BulkBucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.IDENTIFIER;
import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.UTC_TIMEZONE;

public class TableSchemaWriterCreator implements Serializable {

    public TableSchemaIdentity identity;

    public LakeSoulKeyGen keyGen;

    public List<String> primaryKeys;

    public List<String> partitionKeyList;

    public OutputFileConfig outputFileConfig;

    public CdcPartitionComputer partitionComputer;

    public BucketAssigner<RowData, String> bucketAssigner;

    public LakeSoulCDCComparator comparator;

    public Path tableLocation;
    public BulkWriter.Factory<RowData> writerFactory;
    public static TableSchemaWriterCreator create(
            TableId tableId,
            RowType rowType,
            String tableLocation,
            List<String> primaryKeys,
            List<String> partitionKeyList,
            ClassLoader userClassLoader,
            Configuration conf) throws IOException {
        TableSchemaWriterCreator info = new TableSchemaWriterCreator();
        info.identity = new TableSchemaIdentity(tableId, rowType, tableLocation, primaryKeys, partitionKeyList);
        info.primaryKeys = primaryKeys;
        info.partitionKeyList = partitionKeyList;
        info.outputFileConfig = OutputFileConfig.builder().build();

        info.keyGen = new LakeSoulKeyGen(rowType, info.primaryKeys.toArray(new String[0]));
        RecordComparator recordComparator = info.keyGen.getComparator().newInstance(userClassLoader);
        info.comparator = new LakeSoulCDCComparator(recordComparator);

        info.partitionComputer = new CdcPartitionComputer(
                "default",
                rowType.getFieldNames().toArray(new String[0]),
                rowType,
                partitionKeyList.toArray(new String[0]),
                conf.getBoolean(LakeSoulSinkOptions.USE_CDC)
        );

        info.bucketAssigner = new FlinkBucketAssigner(info.partitionComputer);

        info.tableLocation = FlinkUtil.makeQualifiedPath(tableLocation);
        info.writerFactory = ParquetRowDataBuilder.createWriterFactory(
                        rowType,
                        getParquetConfiguration(conf),
                        conf.get(UTC_TIMEZONE));

        return info;
    }

    public BucketWriter<RowData, String> createBucketWriter() throws IOException {
        return new BulkBucketWriter<>(
                FileSystem.get(tableLocation.toUri()).createRecoverableWriter(), writerFactory);
    }

    public static org.apache.hadoop.conf.Configuration getParquetConfiguration(ReadableConfig options) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> conf.set(IDENTIFIER + "." + k, v.toString()));
        return conf;
    }
}
