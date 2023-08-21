// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.tables

import com.dmetasoul.lakesoul.meta.MetaUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.lakesoul.SnapshotManagement

object LakeSoulTableTestUtils {
  /** A utility method to access the private constructor of [[LakeSoulTable]] in tests. */
  def createTable(df: DataFrame, snapshotManagement: SnapshotManagement): LakeSoulTable =
    new LakeSoulTable(df, snapshotManagement)

  lazy val dataBase = MetaUtils.DATA_BASE

}
