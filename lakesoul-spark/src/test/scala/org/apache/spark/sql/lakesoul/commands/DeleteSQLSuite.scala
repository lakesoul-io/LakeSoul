// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeleteSQLSuite extends DeleteSuiteBase with LakeSoulSQLCommandTest {

  override protected def executeDelete(target: String, where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"DELETE FROM $target $whereClause")
  }

}
