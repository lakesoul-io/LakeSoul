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

import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.test.SharedSparkSession

class FragmentValueSuite extends SharedSparkSession {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    MetaTableManage.initDatabaseAndTables()
  }

  test("short schema should not be split") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      withSQLConf(LakeSoulSQLConf.META_MAX_SIZE_PER_VALUE.key -> "500") {
        Seq((1, 1, 1)).toDF("hash", "value", "range")
          .write
          .format("lakesoul")
          .save(tablePath)

        val tableId = SnapshotManagement(tablePath).getTableInfoOnly.table_id
        val fragmentValues = FragmentValue.getFragmentValue(tableId)
        assert(fragmentValues.isEmpty)
      }
    })
  }

  test("long schema should be split") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      withSQLConf(LakeSoulSQLConf.META_MAX_SIZE_PER_VALUE.key -> "10") {
        Seq((1, 1, 1)).toDF("hash", "value", "range")
          .write
          .format("lakesoul")
          .save(tablePath)

        val tableId = SnapshotManagement(tablePath).getTableInfoOnly.table_id
        val fragmentValues = FragmentValue.getFragmentValue(tableId)
        assert(fragmentValues.nonEmpty)
      }
    })
  }


}
