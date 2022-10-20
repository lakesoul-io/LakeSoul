/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.writer;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.lakesoul.types.JsonSourceRecord;
import org.apache.flink.lakesoul.types.LakeSoulRecordConvert;
import org.apache.flink.lakesoul.types.LakeSoulRowDataWrapper;
import org.apache.flink.lakesoul.types.TableSchemaIdentity;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.SERVER_TIME_ZONE;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.USE_CDC;

public class LakeSoulMultiTableSinkWriter extends AbstractLakeSoulMultiTableSinkWriter<JsonSourceRecord> {

    private final LakeSoulRecordConvert converter;

    public LakeSoulMultiTableSinkWriter(int subTaskId,
                                        SinkWriterMetricGroup metricGroup,
                                        LakeSoulWriterBucketFactory bucketFactory,
                                        RollingPolicy<RowData, String> rollingPolicy,
                                        OutputFileConfig outputFileConfig,
                                        Sink.ProcessingTimeService processingTimeService,
                                        long bucketCheckInterval,
                                        ClassLoader userClassLoader,
                                        Configuration conf) {
        super(subTaskId, metricGroup, bucketFactory, rollingPolicy, outputFileConfig, processingTimeService,
              bucketCheckInterval, userClassLoader, conf);
        this.converter = new LakeSoulRecordConvert(conf.getBoolean(USE_CDC), conf.getString(SERVER_TIME_ZONE));
    }

    private TableSchemaIdentity getIdentity(RowType rowType, JsonSourceRecord element) {
        return new TableSchemaIdentity(
                element.getTableId(),
                rowType,
                element.getTableLocation(),
                element.getPrimaryKeys(),
                element.getPartitionKeys()
        );
    }

    @Override
    protected List<Tuple2<TableSchemaIdentity, RowData>> extractTableSchemaAndRowData(JsonSourceRecord element) throws Exception {
        LakeSoulRowDataWrapper wrapper = converter.toLakeSoulDataType(element);
        if (wrapper.getOp().equals("insert")) {
            return Collections.singletonList(Tuple2.of(getIdentity(wrapper.getAfterType(), element),
                                                       wrapper.getAfter()));
        } else if (wrapper.getOp().equals("delete")) {
            return Collections.singletonList(Tuple2.of(getIdentity(wrapper.getBeforeType(), element),
                                                       wrapper.getBefore()));
        } else {
            return Arrays.asList(Tuple2.of(getIdentity(wrapper.getBeforeType(), element), wrapper.getBefore()),
                                 Tuple2.of(getIdentity(wrapper.getAfterType(), element), wrapper.getAfter()));
        }
    }
}
