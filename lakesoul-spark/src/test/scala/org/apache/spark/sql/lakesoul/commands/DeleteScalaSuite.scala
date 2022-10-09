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

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul
import com.dmetasoul.lakesoul.tables.{LakeSoulTable, LakeSoulTableTestUtils}
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.{Row, functions}

class DeleteScalaSuite extends DeleteSuiteBase with LakeSoulSQLCommandTest {

  import testImplicits._

  test("delete cached table by path") {
    Seq((2, 2), (1, 4)).toDF("key", "value")
      .write.mode("overwrite").format("lakesoul").save(tempPath)
    spark.read.format("lakesoul").load(tempPath).cache()
    spark.read.format("lakesoul").load(tempPath).collect()
    executeDelete(s"lakesoul.default.`$tempPath`", where = "key = 2")
    checkAnswer(spark.read.format("lakesoul").load(tempPath), Row(1, 4) :: Nil)
  }

  test("delete usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = LakeSoulTable.forPath(tempPath)
    table.delete()
    checkAnswer(readLakeSoulTable(tempPath), Nil)
  }

  test("delete usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = LakeSoulTable.forPath(tempPath)
    table.delete("key = 1 or key = 2")
    checkAnswer(readLakeSoulTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("delete usage test - with Column condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = lakesoul.tables.LakeSoulTable.forPath(tempPath)
    table.delete(functions.expr("key = 1 or key = 2"))
    checkAnswer(readLakeSoulTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeDelete(target: String, where: String = null): Unit = {

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None // just table name
        case tableName :: alias :: Nil => // tablename SPACE alias OR tab SPACE lename
          val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
          if (!alias.forall(ordinary.contains)) {
            (tableName + " " + alias) -> None
          } else {
            tableName -> Some(alias)
          }
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    val lakeSoulTable: LakeSoulTable = {
      val (tableNameOrPath, optionalAlias) = parse(target)
      val isPath: Boolean = tableNameOrPath.startsWith("lakesoul.")
      val table = if (isPath) {
        val path = tableNameOrPath.stripPrefix("lakesoul.default.`").stripSuffix("`")
        lakesoul.tables.LakeSoulTable.forPath(spark, path)
      } else {
        LakeSoulTableTestUtils.createTable(spark.table(tableNameOrPath),
          SnapshotManagement(tableNameOrPath))
      }
      optionalAlias.map(table.as).getOrElse(table)
    }

    if (where != null) {
      lakeSoulTable.delete(where)
    } else {
      lakeSoulTable.delete()
    }
  }

}
