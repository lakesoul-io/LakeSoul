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

package org.apache.spark.sql.lakesoul.material_view

import com.dmetasoul.lakesoul.meta.MetaUtils
import org.apache.spark.sql.lakesoul.material_view.ConstructQueryInfo._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class AggregateInfo {
  private var aggTables: Set[String] = Set.empty[String]
  private var aggColumns: Set[String] = Set.empty[String]

  private val aggEqualConditions: mutable.Set[(String, String)] = mutable.Set[(String, String)]()
  private val aggOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()

  def setAggEqualCondition(left: String, right: String): Unit = {
    if (left.compareTo(right) <= 0) {
      aggEqualConditions.add(left, right)
    } else {
      aggEqualConditions.add(right, left)
    }
  }

  def setAggOtherCondition(cond: String): Unit = {
    aggOtherConditions += cond
  }

  def setAggInfo(tables: Set[String], cols: Set[String]): Unit = {
    aggTables = tables
    aggColumns = cols
  }

  def buildAggregateDetail(tables: Map[String, String], asInfo: Map[String, String]): AggregateDetail = {
    AggregateDetail(
      aggTables.map(getFinalStringByReplace(_, tables, asInfo)),
      aggColumns.map(getFinalStringByReplace(_, tables, asInfo)),
      aggEqualConditions.map(m =>
        (getFinalStringByReplace(m._1, tables, asInfo),
          getFinalStringByReplace(m._2, tables, asInfo)))
        .toSet,
      aggOtherConditions.map(getFinalStringByReplace(_, tables, asInfo)).toSet)
  }


}


object AggregateInfo {
  def buildDetail(str: String): AggregateDetail = {
    val split = str.split(MetaUtils.LAKE_SOUL_SEP_03, -1)
    assert(split.length == 4)
    val equal = if (split(2).equals("")) {
      Set.empty[(String, String)]
    } else {
      split(2).split(MetaUtils.LAKE_SOUL_SEP_02, -1).map(m => {
        val arr = m.split(MetaUtils.LAKE_SOUL_SEP_01)
        assert(arr.length == 2)
        (arr(0), arr(1))
      }).toSet
    }
    val other = if (split(3).equals("")) {
      Set.empty[String]
    } else {
      split(3).split(MetaUtils.LAKE_SOUL_SEP_02).toSet
    }

    AggregateDetail(
      split(0).split(MetaUtils.LAKE_SOUL_SEP_02).toSet,
      split(1).split(MetaUtils.LAKE_SOUL_SEP_02).toSet,
      equal,
      other)
  }
}


case class AggregateDetail(tables: Set[String],
                           columns: Set[String],
                           equalCondition: Set[(String, String)],
                           otherCondition: Set[String]) {
  override def toString: String = {
    val equal = equalCondition
      .map(m => m._1 + MetaUtils.LAKE_SOUL_SEP_01 + m._2)
      .mkString(MetaUtils.LAKE_SOUL_SEP_02)
    val other = otherCondition.mkString(MetaUtils.LAKE_SOUL_SEP_02)

    Seq(tables.mkString(MetaUtils.LAKE_SOUL_SEP_02),
      columns.mkString(MetaUtils.LAKE_SOUL_SEP_02),
      equal,
      other)
      .mkString(MetaUtils.LAKE_SOUL_SEP_03)
  }
}