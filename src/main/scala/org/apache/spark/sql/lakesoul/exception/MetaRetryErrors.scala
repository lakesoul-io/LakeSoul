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

package org.apache.spark.sql.lakesoul.exception

import com.dmetasoul.lakesoul.meta.MetaUtils

object MetaRetryErrors {

  def failedUpdateCommitTimestampException(table_name: String,
                                           commit_id: String): MetaRetryException = {
    new MetaRetryException(
      s"""
         |Error: Failed to update commit timestamp because of timeout.
         |You may need to reset option COMMIT_TIMEOUT, default is ${MetaUtils.COMMIT_TIMEOUT}ms.
         |Error table: $table_name, commit_id: $commit_id .
       """.stripMargin,
      commit_id)
  }

  def failedMarkCommitTagException(table_name: String,
                                   commit_id: String): MetaRetryException = {
    new MetaRetryException(
      s"""
         |Error: Failed to mark commit tag because of timeout.
         |You may need to reset option COMMIT_TIMEOUT, now is ${MetaUtils.COMMIT_TIMEOUT}ms.
         |Error table: $table_name, commit_id: $commit_id .
       """.stripMargin,
      commit_id)
  }

  def tooManyCommitException(): MetaRetryException = {
    new MetaRetryException(
      s"""
         |Warn: commit meta data failed, it may had too many commits at the same time.
       """.stripMargin)
  }


}
