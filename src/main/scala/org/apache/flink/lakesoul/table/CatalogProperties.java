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

package org.apache.flink.lakesoul.table;

import org.apache.flink.configuration.ConfigOption;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.ConfigOptions.key;

public class CatalogProperties {

    private CatalogProperties() {
    }
    //public static final String LakesoulDatabaseName="test_lakesoul_meta";
    public static final ConfigOption<String> PATH =
            key("path").stringType().noDefaultValue().withDescription("The path of a directory");

    public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS = "client.pool.cache.eviction-interval-ms";
    public static final long CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);
    public static final String LOCK_HEARTBEAT_INTERVAL_MS = "lock.heartbeat-interval-ms";
    public static final long LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = TimeUnit.SECONDS.toMillis(3);
    public static final String LOCK_TABLE = "lock.table";
    public static final String USER = "user";

}
