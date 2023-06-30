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

package org.apache.flink.lakesoul.types;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

public class BinaryDebeziumDeserializationSchema implements DebeziumDeserializationSchema<BinarySourceRecord> {

    LakeSoulRecordConvert convert;
    String basePath;

    public BinaryDebeziumDeserializationSchema(LakeSoulRecordConvert convert, String basePath) {
        this.convert = convert;
        this.basePath = basePath;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<BinarySourceRecord> collector) throws Exception {
        BinarySourceRecord binarySourceRecord = BinarySourceRecord.fromMysqlSourceRecord(sourceRecord, this.convert, this.basePath);
        if (binarySourceRecord != null) collector.collect(binarySourceRecord);
    }

    @Override
    public TypeInformation<BinarySourceRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<BinarySourceRecord>() {
        });
    }
}
