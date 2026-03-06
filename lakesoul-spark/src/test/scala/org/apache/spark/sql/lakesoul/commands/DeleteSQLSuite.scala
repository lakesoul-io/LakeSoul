// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeleteSQLSuite extends DeleteSuiteBase with LakeSoulSQLCommandTest {

  import testImplicits._

  override protected def executeDelete(target: String, where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"DELETE FROM $target $whereClause")
  }

  //Seq(true, false).foreach { isPartitioned =>
  // test(s"basic case - delete from a LakeSoul table by path - Partition=f") {
  //    withTable("starTable") {
  //    val partitions = if (false) "key" :: Nil else Nil
  //  val input = Seq((2, 2), (1, 4), (1, 1), (0, 3)).toDF("key", "value")
  // append(input, partitions)

  // checkDelete(Some("value = 4 and key = 3"),
  //  Row(2, 2) :: Row(1, 4) :: Row(1, 1) :: Row(0, 3) :: Nil,
  //  Seq("key", "value"))
  //checkDelete(Some("value = 4 and key = 1"),
  // Row(2, 2) :: Row(1, 1) :: Row(0, 3) :: Nil,
  // Seq("key", "value"))
  //  checkDelete(Some("value = 2 or key = 1"),
  //   Row(0, 3) :: Nil,
  //  Seq("key", "value"))
  // checkDelete(Some("key = 0 or value = 99"), Nil,
  //  Seq("key", "value"))
  //}
  // }
  //}

}
