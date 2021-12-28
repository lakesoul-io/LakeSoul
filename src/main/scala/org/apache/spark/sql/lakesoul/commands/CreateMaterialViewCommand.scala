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

import com.dmetasoul.lakesoul.meta.MetaVersion
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{SnapshotManagement, LakeSoulOptions, LakeSoulUtils}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class CreateMaterialViewCommand(viewName: String,
                                     viewPath: String,
                                     sqlText: String,
                                     rangePartitions: String,
                                     hashPartitions: String,
                                     hashBucketNum: String,
                                     autoUpdate: Boolean) extends RunnableCommand with Command {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    LakeSoulUtils.executeWithoutQueryRewrite(sparkSession) {
      val snapshotManagement = SnapshotManagement(viewPath)
      snapshotManagement.withNewTransaction(tc => {
        //fast failed if view name already exists
        if (MetaVersion.isShortTableNameExists(viewName)._1) {
          throw LakeSoulErrors.tableExistsException(viewName)
        }

        val options = Map(
          LakeSoulOptions.RANGE_PARTITIONS -> rangePartitions,
          LakeSoulOptions.HASH_PARTITIONS -> hashPartitions,
          LakeSoulOptions.HASH_BUCKET_NUM -> hashBucketNum,
          LakeSoulOptions.SHORT_TABLE_NAME -> viewName,
          LakeSoulOptions.CREATE_MATERIAL_VIEW -> "true",
          LakeSoulOptions.MATERIAL_SQL_TEXT -> sqlText,
          LakeSoulOptions.MATERIAL_AUTO_UPDATE -> autoUpdate.toString
        )

        val data = sparkSession.sql(sqlText)

        val (newFiles, deletedFiles) = WriteIntoTable(
          snapshotManagement,
          SaveMode.ErrorIfExists,
          new LakeSoulOptions(options, sparkSession.sessionState.conf),
          configuration = Map.empty, //table.properties,
          data).write(tc, sparkSession)

        tc.commit(newFiles, deletedFiles)
      })

    }

    Nil
  }

}
