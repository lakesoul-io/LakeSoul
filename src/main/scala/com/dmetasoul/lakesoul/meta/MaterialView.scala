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

import org.apache.spark.sql.lakesoul.material_view.ConstructQueryInfo
import org.apache.spark.sql.lakesoul.utils.{MaterialViewInfo, RelationTable}

import scala.util.matching.Regex

object MaterialView {
  private val cassandraConnector = MetaUtils.cassandraConnector
  private val database = MetaUtils.DATA_BASE

  //check whether material view exists or not
  def isMaterialViewExists(view_name: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_name from $database.material_view
           |where view_name='$view_name'
        """.stripMargin)
      try {
        res.one().getString("table_name")
      } catch {
        case _: NullPointerException => return false
        case e: Exception => throw e
      }
      true
    })
  }


  def addMaterialView(view_name: String,
                      table_name: String,
                      table_id: String,
                      relation_tables: String,
                      sql_text: String,
                      auto_update: Boolean,
                      view_info_index: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      val format_sql_text = MetaUtils.formatSqlTextToCassandra(sql_text)
      session.execute(
        s"""
           |insert into $database.material_view
           |(view_name,table_name,table_id,relation_tables,sql_text,auto_update,view_info)
           |values ('$view_name', '$table_name', '$table_id', '$relation_tables', '$format_sql_text',
           |$auto_update, '$view_info_index')
        """.stripMargin)
    })
  }


  def updateMaterialView(view_name: String,
                         relation_tables: String,
                         auto_update: Boolean): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |update $database.material_view
           |set relation_tables='$relation_tables',auto_update=$auto_update
           |where view_name='$view_name'
        """.stripMargin)
    })
  }

  def deleteMaterialView(view_name: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.material_view where view_name='$view_name'
        """.stripMargin)
    })
  }

  def getMaterialViewInfo(view_name: String): Option[MaterialViewInfo] = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_id,sql_text,relation_tables,auto_update,view_info from $database.material_view
           |where view_name='$view_name'
        """.stripMargin).one()
      try {
        val table_id = res.getString("table_id")
        val view_info_index = res.getString("view_info")

        //if viewInfo is some fragment index, we should get true value and joint them
        val parttern = new Regex("\\w{8}(-\\w{4}){3}-\\w{12}")
        val view_info =
          if (parttern.findFirstIn(view_info_index).isDefined
            && !view_info_index.contains("{")) {
            FragmentValue.getEntireValue(table_id, view_info_index)
          } else {
            view_info_index
          }


        Some(MaterialViewInfo(
          view_name,
          MetaUtils.formatSqlTextFromCassandra(res.getString("sql_text")),
          res.getString("relation_tables").split(",").map(m => RelationTable.build(m)),
          res.getBool("auto_update"),
          false,
          ConstructQueryInfo.buildInfo(view_info)))
      } catch {
        case _: NullPointerException => return None
        case e: Exception => throw e
      }
    })
  }


  def updateMaterialRelationInfo(table_id: String,
                                 table_name: String,
                                 new_views: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |insert into $database.material_relation
           |(table_id,table_name,material_views)
           |values ('$table_id', '$table_name', '$new_views')
        """.stripMargin)
    })
  }

  def getMaterialRelationInfo(table_id: String): String = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select material_views from $database.material_relation where table_id='$table_id'
        """.stripMargin)
      try {
        res.one().getString("material_views")
      } catch {
        case _: NullPointerException => return ""
        case e: Exception => throw e
      }
    })
  }

  def deleteMaterialRelationInfo(table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.material_relation where table_id='$table_id'
        """.stripMargin)
    })
  }


}
