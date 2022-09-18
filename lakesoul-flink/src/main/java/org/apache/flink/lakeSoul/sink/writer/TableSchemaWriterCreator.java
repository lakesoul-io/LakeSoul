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

package org.apache.flink.lakeSoul.sink.writer;

import io.debezium.relational.TableId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.lakeSoul.sink.bucket.FlinkBucketAssigner;
import org.apache.flink.lakeSoul.sink.bucket.LakeSoulRollingPolicyImpl;
import org.apache.flink.lakeSoul.sink.bucket.bulkFormat.DefaultBulkFormatBuilder;
import org.apache.flink.lakeSoul.sink.partition.CdcPartitionComputer;
import org.apache.flink.lakeSoul.table.LakeSoulTableWriter;
import org.apache.flink.lakeSoul.tool.LakeSoulKeyGen;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.filesystem.stream.PartitionCommitPredicate;
import org.apache.flink.table.types.logical.RowType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.IDENTIFIER;
import static org.apache.flink.formats.parquet.ParquetFileFormatFactory.UTC_TIMEZONE;
import static org.apache.flink.lakeSoul.tool.LakeSoulSinkOptions.*;

public class TableSchemaWriterCreator implements Serializable {

    public static final class TableSchemaIdentity {
        public TableId tableId;

        public RowType rowType;

        public TableSchemaIdentity(TableId tableId, RowType rowType) {
            this.tableId = tableId;
            this.rowType = rowType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableSchemaIdentity that = (TableSchemaIdentity) o;
            return tableId.equals(that.tableId) && rowType.equals(that.rowType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableId, rowType);
        }
    }

    public TableSchemaIdentity identity;

    public LakeSoulKeyGen keyGen;

    public DefaultBulkFormatBuilder<RowData> bucketsBuilder;

    public List<String> primaryKeys;

    public List<String> partitionKeyList;

    public OutputFileConfig outputFileConfig;

    public CdcPartitionComputer partitionComputer;

    public BucketAssigner<RowData, String> bucketAssigner;

    public Path tableLocation;

    public static TableSchemaWriterCreator create(
            TableId tableId,
            RowType rowType,
            String tableLocation,
            List<String> primaryKeys,
            List<String> partitionKeyList,
            Configuration conf) throws IOException {
        TableSchemaWriterCreator info = new TableSchemaWriterCreator();
        info.identity = new TableSchemaIdentity(tableId, rowType);
        info.primaryKeys = primaryKeys;
        info.partitionKeyList = partitionKeyList;
        info.outputFileConfig = OutputFileConfig.builder().build();

        info.keyGen = new LakeSoulKeyGen(rowType, conf, info.primaryKeys.toArray(new String[0]));

        info.partitionComputer = new CdcPartitionComputer(
                "default",
                rowType.getFieldNames().toArray(new String[0]),
                rowType,
                partitionKeyList.toArray(new String[0]),
                true
        );

        info.bucketAssigner = new FlinkBucketAssigner(info.partitionComputer);

        Path path = new Path(tableLocation);
        FileSystem fileSystem = path.getFileSystem();
        path = path.makeQualified(fileSystem);
        info.tableLocation = path;
        LakeSoulRollingPolicyImpl rollingPolicy = new LakeSoulRollingPolicyImpl(
                conf.getLong(FILE_ROLLING_SIZE), conf.getLong(FILE_ROLLING_TIME), info.keyGen);
        info.bucketsBuilder = new DefaultBulkFormatBuilder<>(
                path,
                ParquetRowDataBuilder.createWriterFactory(
                        rowType,
                        getParquetConfiguration(conf),
                        conf.get(UTC_TIMEZONE)),
                info.bucketAssigner
        ).withOutputFileConfig(info.outputFileConfig)
                .withRollingPolicy(rollingPolicy);

        return info;
    }

    private static org.apache.hadoop.conf.Configuration getParquetConfiguration(ReadableConfig options) {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> conf.set(IDENTIFIER + "." + k, v.toString()));
        return conf;
    }

    public LakesSoulOneTableWriter<RowData> createTableSchemaWriter(
            int bucketCheckInterval,
            ClassLoader classLoader,
            Configuration conf,
            StreamingRuntimeContext context) {
        return new LakesSoulOneTableWriter<>(bucketCheckInterval, bucketsBuilder,
                PartitionCommitPredicate.create(conf, classLoader, partitionKeyList),
                context);
    }
}
