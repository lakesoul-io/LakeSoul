// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import java.util.UUID

object StreamingRecord {

  val dbManager = new DBManager()

  def getBatchId(tableId: String, queryId: String): Long = {
    try {
      val commitId = DBUtil.toJavaUUID(dbManager.selectByTableId(tableId).getCommitId)
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
