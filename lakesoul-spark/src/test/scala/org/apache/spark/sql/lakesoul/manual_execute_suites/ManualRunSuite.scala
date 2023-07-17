// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.manual_execute_suites

object ManualRunSuite {
  def main(args: Array[String]): Unit = {
    new CompactionDoNotChangeResult().run()
    new MergeOneFileResult().run()
    new ShuffleJoinSuite().run()
    new UpsertAfterCompaction().run()
    new UpsertWithDuplicateDataAndFields().run()
    new UpsertWithDuplicateDataByDifferent().run()
    new UpsertWithDuplicateDataBySame().run()
  }
}
