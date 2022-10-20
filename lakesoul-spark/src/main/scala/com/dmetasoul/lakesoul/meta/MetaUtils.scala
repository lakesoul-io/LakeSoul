/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dmetasoul.lakesoul.meta

import org.apache.spark.internal.Logging

object MetaUtils extends Logging {

  lazy val DEFAULT_RANGE_PARTITION_VALUE: String = "-5"

  var DATA_BASE: String = "test_lakesoul_meta";;

  lazy val MAX_COMMIT_ATTEMPTS: Int = 5
  lazy val DROP_TABLE_WAIT_SECONDS: Int = 1
  lazy val PART_MERGE_FILE_MINIMUM_NUM:Int = 5

  /** get partition key string from scala Map */
  def getPartitionKeyFromMap(cols: Map[String, String]): String = {
    if (cols.isEmpty) {
      DEFAULT_RANGE_PARTITION_VALUE
    } else {
      cols.toList.sorted.map(list => {
        list._1 + "=" + list._2
      }).mkString(",")
    }
  }

  /** get partition key Map from string */
  def getPartitionMapFromKey(range_value: String): Map[String, String] = {
    var partition_values = Map.empty[String, String]
    if (!range_value.equals(DEFAULT_RANGE_PARTITION_VALUE)) {
      val range_list = range_value.split(",")
      for (range <- range_list) {
        val parts = range.split("=")
        partition_values = partition_values ++ Map(parts(0) -> parts(1))
      }
    }
    partition_values
  }

}

