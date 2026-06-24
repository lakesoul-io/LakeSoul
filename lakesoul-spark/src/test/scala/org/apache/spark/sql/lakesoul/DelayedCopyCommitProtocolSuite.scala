// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NON_PARTITION_TABLE_PART_DESC
import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DelayedCopyCommitProtocolSuite extends AnyFunSuite {

  test("destinationRelativePath keeps the compact-relative path for compacted files") {
    val protocol = new DelayedCopyCommitProtocol(Nil, "job", "/tmp/dst", None)
    val srcFile = DataFileInfo(
      "range=1",
      "file:/tmp/table/compact_123456/range=1/part-0000.parquet",
      "add",
      10L,
      0L)

    assert(protocol.destinationRelativePath(srcFile) == "range=1/part-0000.parquet")
  }

  test("destinationRelativePath rebuilds a partition-relative path for uncompacted files") {
    val protocol = new DelayedCopyCommitProtocol(Nil, "job", "/tmp/dst", None)
    val srcFile = DataFileInfo(
      "range=1,date=2023-01-01",
      "file:/tmp/table/range=1/date=2023-01-01/part-0000.parquet",
      "add",
      10L,
      0L)

    assert(protocol.destinationRelativePath(srcFile) == "range=1/date=2023-01-01/part-0000.parquet")
  }

  test("destinationRelativePath keeps only the file name for non-partitioned uncompacted files") {
    val protocol = new DelayedCopyCommitProtocol(Nil, "job", "/tmp/dst", None)
    val srcFile = DataFileInfo(
      LAKESOUL_NON_PARTITION_TABLE_PART_DESC,
      "file:/tmp/table/part-0000.parquet",
      "add",
      10L,
      0L)

    assert(protocol.destinationRelativePath(srcFile) == "part-0000.parquet")
  }
}
