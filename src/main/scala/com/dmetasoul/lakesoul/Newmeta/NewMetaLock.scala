/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta

import com.dmetasoul.lakesoul.Newmeta.MetaCommon

object NewMetaLock {
  private val cassandraConnector = MetaCommon.cassandraConnector
  private val database = MetaCommon.DATA_BASE

  def lock(lockId: String, commitId: String): (Boolean, String) = {
    cassandraConnector.withSessionDo(session => {
      val re = session.execute(
        s"""
           |insert into $database.lock_info(lock_id,commit_id)
           |values('$lockId','$commitId')
           |if not exists
      """.stripMargin)
      if (re.wasApplied()) {
        (true, "")
      } else {
        (false, re.one().getString("commit_id"))
      }
    })
  }

  def unlock(lockId: String, commitId: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.lock_info
           |where lock_id='$lockId'
           |if commit_id='$commitId'
      """.stripMargin).wasApplied()
    })
  }

}
