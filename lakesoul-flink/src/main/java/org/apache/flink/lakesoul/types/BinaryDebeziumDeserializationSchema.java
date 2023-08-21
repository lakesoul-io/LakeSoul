// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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
