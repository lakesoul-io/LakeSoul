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
package org.apache.flink.lakeSoul.test;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.lakeSoul.sink.LakeSoulDDLSink;
import org.apache.flink.lakeSoul.sink.LakeSoulDataType;
import org.apache.flink.lakeSoul.sink.LakeSoulRecordConvert;
import org.apache.flink.lakeSoul.types.JsonDebeziumDeserializationSchema;
import org.apache.flink.lakeSoul.types.JsonSourceRecord;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class JsonRecordForLakeSoulTest {
    public static void main(String[] args) throws Exception {

        JsonSourceRecord item = new JsonSourceRecord("mysql_binlog_source.sms.cdc",
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int16\",\"optional\":false,\"field\":\"range\"}],\"optional\":false,\"name\":\"mysql_binlog_source.sms.cdc.Key\"},\"payload\":{\"id\":3,\"range\":2}}",
                "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int16\",\"optional\":false,\"field\":\"range\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"value5\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"value1\"},{\"type\":\"string\",\"optional\":true,\"field\":\"value2\"},{\"type\":\"string\",\"optional\":true,\"field\":\"value3\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"value4\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Year\",\"version\":1,\"field\":\"value6\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"10\"},\"field\":\"value7\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"9\"},\"field\":\"value8\"}],\"optional\":true,\"name\":\"mysql_binlog_source.sms.cdc.Value\",\"field\":\"before\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int16\",\"optional\":false,\"field\":\"range\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Date\",\"version\":1,\"field\":\"value5\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"value1\"},{\"type\":\"string\",\"optional\":true,\"field\":\"value2\"},{\"type\":\"string\",\"optional\":true,\"field\":\"value3\"},{\"type\":\"bytes\",\"optional\":true,\"field\":\"value4\"},{\"type\":\"int32\",\"optional\":true,\"name\":\"io.debezium.time.Year\",\"version\":1,\"field\":\"value6\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"0\",\"connect.decimal.precision\":\"10\"},\"field\":\"value7\"},{\"type\":\"bytes\",\"optional\":true,\"name\":\"com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.data.Decimal\",\"version\":1,\"parameters\":{\"scale\":\"4\",\"connect.decimal.precision\":\"9\"},\"field\":\"value8\"}],\"optional\":true,\"name\":\"mysql_binlog_source.sms.cdc.Value\",\"field\":\"after\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"version\"},{\"type\":\"string\",\"optional\":false,\"field\":\"connector\"},{\"type\":\"string\",\"optional\":false,\"field\":\"name\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"ts_ms\"},{\"type\":\"string\",\"optional\":true,\"name\":\"io.debezium.data.Enum\",\"version\":1,\"parameters\":{\"allowed\":\"true,last,false\"},\"default\":\"false\",\"field\":\"snapshot\"},{\"type\":\"string\",\"optional\":false,\"field\":\"db\"},{\"type\":\"string\",\"optional\":true,\"field\":\"sequence\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"server_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"gtid\"},{\"type\":\"string\",\"optional\":false,\"field\":\"file\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"pos\"},{\"type\":\"int32\",\"optional\":false,\"field\":\"row\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"thread\"},{\"type\":\"string\",\"optional\":true,\"field\":\"query\"}],\"optional\":false,\"name\":\"io.debezium.connector.mysql.Source\",\"field\":\"source\"},{\"type\":\"string\",\"optional\":false,\"field\":\"op\"},{\"type\":\"int64\",\"optional\":true,\"field\":\"ts_ms\"},{\"type\":\"struct\",\"fields\":[{\"type\":\"string\",\"optional\":false,\"field\":\"id\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"total_order\"},{\"type\":\"int64\",\"optional\":false,\"field\":\"data_collection_order\"}],\"optional\":true,\"field\":\"transaction\"}],\"optional\":false,\"name\":\"mysql_binlog_source.sms.cdc.Envelope\"},\"payload\":{\"before\":null,\"after\":{\"id\":3,\"range\":2,\"value5\":19255,\"value1\":\"dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdHRlc3Q=\",\"value2\":\"testtesttesttesttesttesttesttest\",\"value3\":\"testtesttesttesttesttesttest\",\"value4\":\"dGVzdHRlc3R0ZXN0dGVzdHRlc3R0ZXN0dGVzdA==\",\"value6\":2022,\"value7\":\"Hg==\",\"value8\":\"BjRI\"},\"source\":{\"version\":\"1.5.4.Final\",\"connector\":\"mysql\",\"name\":\"mysql_binlog_source\",\"ts_ms\":1663661643000,\"snapshot\":\"false\",\"db\":\"sms\",\"sequence\":null,\"table\":\"cdc\",\"server_id\":1,\"gtid\":\"e1518d38-2aa6-11ed-a368-0242ac110002:145\",\"file\":\"mysql-bin.000007\",\"pos\":4361,\"row\":0,\"thread\":null,\"query\":null},\"op\":\"c\",\"ts_ms\":1663661643078,\"transaction\":null}}"
                );
        LakeSoulRecordConvert lsrc = new LakeSoulRecordConvert();
        LakeSoulDataType lsdt = null;
        lsdt =lsrc.toLakeSoulDataType(item);
        System.out.println(lsdt);

    }
}
