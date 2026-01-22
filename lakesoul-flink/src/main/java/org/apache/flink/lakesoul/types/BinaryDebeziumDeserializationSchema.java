// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.types;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;

public class BinaryDebeziumDeserializationSchema implements DebeziumDeserializationSchema<BinarySourceRecord> {

    private static final long serialVersionUID = -3248249461777452263L;
    LakeSoulRecordConvert convert;
    String basePath;
    String sinkDBName;

    public BinaryDebeziumDeserializationSchema(LakeSoulRecordConvert convert, String basePath, String sinkDBName) {
        this.convert = convert;
        this.basePath = basePath;
        this.sinkDBName = sinkDBName;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<BinarySourceRecord> collector) throws Exception {
        BinarySourceRecord binarySourceRecord = BinarySourceRecord.fromMysqlSourceRecord(sourceRecord, this.convert, this.basePath, this.sinkDBName);
        if (binarySourceRecord != null) collector.collect(binarySourceRecord);
    }

    @Override
    public TypeInformation<BinarySourceRecord> getProducedType() {
        return TypeInformation.of(new TypeHint<BinarySourceRecord>() {
        });
    }
}
