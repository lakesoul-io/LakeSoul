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

package com.dmetasoul.lakesoul.meta

object LakeSoulOptions {

  /** An option to overwrite only the data that matches predicates over partition columns. */
  val REPLACE_WHERE_OPTION = "replaceWhere"
  /** An option to allow automatic schema merging during a write operation. */
  val MERGE_SCHEMA_OPTION = "mergeSchema"
  /** An option to allow overwriting schema and partitioning during an overwrite write operation. */
  val OVERWRITE_SCHEMA_OPTION = "overwriteSchema"

  val PARTITION_BY = "__partition_columns"
  val RANGE_PARTITIONS = "rangePartitions"
  val HASH_PARTITIONS = "hashPartitions"
  val HASH_BUCKET_NUM = "hashBucketNum"

  val SHORT_TABLE_NAME = "shortTableName"

  /** whether it is allowed to use delta file */
  val AllowDeltaFile = "allowDeltaFile"

  val PARTITION_DESC = "partitiondesc"
  val READ_START_TIME = "readstarttime"
  val READ_END_TIME = "readendtime"
  /** An option to allow read type whether snapshot or increamental. */
  val READ_TYPE = "readtype"
  val TIME_ZONE = "timezone"
  val DISCOVERY_INTERVAL = "discoveryinterval"

  object ReadType extends Enumeration {
    val FULL_READ = "fullread"
    val SNAPSHOT_READ = "snapshot"
    val INCREMENTAL_READ = "incremental"
  }
}