// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER
import org.apache.spark.internal.Logging

object MetaUtils extends Logging {

  lazy val DEFAULT_RANGE_PARTITION_VALUE: String = DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC

  var DATA_BASE: String = "test_lakesoul_meta"

  lazy val MAX_COMMIT_ATTEMPTS: Int = 5
  lazy val DROP_TABLE_WAIT_SECONDS: Int = 1
  lazy val PART_MERGE_FILE_MINIMUM_NUM: Int = 5

  /** get partition key string from scala Map */
  def getPartitionKeyFromList(cols: List[(String, String)]): String = {
    if (cols.isEmpty) {
      DEFAULT_RANGE_PARTITION_VALUE
    } else {
      cols.map(list => {
        list._1 + "=" + list._2
      }).mkString(LAKESOUL_RANGE_PARTITION_SPLITTER)
    }
  }

  /** get partition key Map from string */
  def getPartitionMapFromKey(range_value: String): Map[String, String] = {
    var partition_values = Map.empty[String, String]
    if (!range_value.equals(DEFAULT_RANGE_PARTITION_VALUE)) {
      val range_list = range_value.split(LAKESOUL_RANGE_PARTITION_SPLITTER)
      for (range <- range_list) {
        val parts = range.split("=")
        partition_values = partition_values ++ Map(parts(0) -> parts(1))
      }
    }
    partition_values
  }

}

