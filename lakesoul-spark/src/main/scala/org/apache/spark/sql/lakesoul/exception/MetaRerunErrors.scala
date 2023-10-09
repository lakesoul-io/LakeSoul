// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.exception

import com.dmetasoul.lakesoul.meta.PartitionInfoScala


object MetaRerunErrors {

  def fileChangedException(info: PartitionInfoScala,
                           file_path: String,
                           write_version: Long,
                           commit_id: String): MetaRerunException = {
    new MetaRerunException(
      s"""
         |Error: Another job added file "$file_path" in partition: "${info.range_value}" during write_version=$write_version, but your read_version is ${info.version}.
         |Commit id=$commit_id failed to update meta because of data info conflict. Please update and retry.
         |Error table: ${info.table_id}.
       """.stripMargin,
      commit_id)
  }

  def fileDeletedException(info: PartitionInfoScala,
                           file_path: String,
                           write_version: Long,
                           commit_id: String): MetaRerunException = {
    new MetaRerunException(
      s"""
         |Error: File "$file_path" in partition: "${info.range_value}" deleted by another job during write_version=$write_version, but your read_version is ${info.version}.
         |Commit id=$commit_id failed to update meta because of data info conflict. Please retry.
         |Error table: ${info.table_id}.
       """.stripMargin,
      commit_id)
  }

  def partitionChangedException(range_value: String, commit_id: String): MetaRerunException = {
    new MetaRerunException(
      s"""
         |Error: Partition `$range_value` has been changed, it may have another job drop and create a newer one.
       """.stripMargin,
      commit_id)
  }
}
