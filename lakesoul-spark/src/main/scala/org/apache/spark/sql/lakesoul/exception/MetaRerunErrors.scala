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

package org.apache.spark.sql.lakesoul.exception

import org.apache.spark.sql.lakesoul.utils.PartitionInfo

object MetaRerunErrors {

  def fileChangedException(info: PartitionInfo,
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

  def fileDeletedException(info: PartitionInfo,
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
