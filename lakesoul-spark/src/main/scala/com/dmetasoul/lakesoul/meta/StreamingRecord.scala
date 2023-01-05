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

import java.util.UUID

object StreamingRecord {

  val dbManager = new DBManager()

  def getBatchId(tableId: String, queryId: String): Long = {
    try {
      val commitId = dbManager.selectByTableId(tableId).getCommitId
      if (commitId.getMostSignificantBits.equals(UUID.fromString(queryId).getMostSignificantBits)) {
        commitId.getLeastSignificantBits
      } else {
        -1L
      }
    } catch {
      case _: Exception => -1L
    }
  }
}
