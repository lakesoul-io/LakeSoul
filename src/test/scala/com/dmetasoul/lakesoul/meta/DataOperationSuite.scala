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

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.exception.MetaException
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class DataOperationSuite extends QueryTest
  with SharedSparkSession with LakeSoulTestUtils {

  private lazy val cassandraConnector = MetaUtils.cassandraConnector
  private lazy val database = MetaUtils.DATA_BASE

  private def getFileNum(table_id: String, range_id: String): Long = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |select count(*) as num from $database.data_info
           |where table_id='$table_id' and range_id='$range_id'
        """.stripMargin).one().getLong("num")
    })
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MetaTableManage.initDataInfo()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    MetaTableManage.cleanDataInfo()
  }

  test("write and read data info") {
    DataOperation.addNewDataFile(
      table_id = "table_id",
      range_id = "range_id",
      file_path = "file_path1",
      write_version = 1,
      commit_id = "commit_id1",
      size = 111,
      modification_time = 1111,
      file_exist_cols = "file_exist_cols",
      is_base_file = true)

    DataOperation.addNewDataFile(
      table_id = "table_id",
      range_id = "range_id",
      file_path = "file_path2",
      write_version = 2,
      commit_id = "commit_id2",
      size = 222,
      modification_time = 2222,
      file_exist_cols = "file_exist_cols",
      is_base_file = true)


    val fileInfo1 = DataOperation.getSinglePartitionDataInfo(
      table_id = "table_id",
      range_id = "range_id",
      range_value = MetaUtils.DEFAULT_RANGE_PARTITION_VALUE,
      read_version = 1
    )
    assert(
      fileInfo1.size == 1 &&
        fileInfo1.head.file_path.equals("file_path1") &&
        fileInfo1.head.modification_time == 1111 &&
        fileInfo1.head.range_partitions.isEmpty &&
        fileInfo1.head.size == 111
    )

    DataOperation.addNewDataFile(
      table_id = "table_id",
      range_id = "range_id",
      file_path = "file_path3",
      write_version = 3,
      commit_id = "commit_id3",
      size = 333,
      modification_time = 3333,
      file_exist_cols = "file_exist_cols",
      is_base_file = true)

    DataOperation.deleteExpireDataFile(
      table_id = "table_id",
      range_id = "range_id",
      file_path = "file_path1",
      write_version = 3,
      commit_id = "commit_id3",
      modification_time = 3333
    )

    DataOperation.deleteExpireDataFile(
      table_id = "table_id",
      range_id = "range_id",
      file_path = "file_path2",
      write_version = 3,
      commit_id = "commit_id3",
      modification_time = 3333
    )

    val fileInfo2 = DataOperation.getSinglePartitionDataInfo(
      table_id = "table_id",
      range_id = "range_id",
      range_value = MetaUtils.DEFAULT_RANGE_PARTITION_VALUE,
      read_version = 2
    )
    assert(fileInfo2.size == 2)

    val fileInfo3 = DataOperation.getSinglePartitionDataInfo(
      table_id = "table_id",
      range_id = "range_id",
      range_value = MetaUtils.DEFAULT_RANGE_PARTITION_VALUE,
      read_version = 3
    )
    assert(
      fileInfo3.size == 1 &&
        fileInfo3.head.file_path.equals("file_path3") &&
        fileInfo3.head.modification_time == 3333 &&
        fileInfo3.head.size == 333
    )


    assert(getFileNum("table_id", "range_id") == 3)

    DataOperation.removeFileByName("table_id", "range_id", 3)

    assert(getFileNum("table_id", "range_id") == 1)

  }

}
