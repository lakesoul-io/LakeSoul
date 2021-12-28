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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class JoinInfo {
  private var leftTables: Set[String] = _
  private var rightTables: Set[String] = _

  private var joinType: String = _

  private val joinEqualConditions: mutable.Set[(String, String)] = mutable.Set[(String, String)]()
  private val joinOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()

  def setJoinEqualCondition(left: String, right: String): Unit = {
    if (left.compareTo(right) <= 0) {
      joinEqualConditions.add(left, right)
    } else {
      joinEqualConditions.add(right, left)
    }
  }

  def setJoinOtherCondition(cond: String): Unit = {
    joinOtherConditions += cond
  }

  def setJoinInfo(left: Set[String], right: Set[String], jt: String): Unit = {
    leftTables = left
    rightTables = right
    joinType = jt
  }

  def buildJoinDetail(): JoinDetail = {
    JoinDetail(
      leftTables,
      rightTables,
      joinType,
      joinEqualConditions.toSet,
      joinOtherConditions.toSet)
  }


}


object JoinInfo {
  def buildEmptyInfo(): JoinDetail = {
    JoinDetail(
      Set.empty[String],
      Set.empty[String],
      "",
      Set.empty[(String, String)],
      Set.empty[String])
  }


  def build(str: String): JoinDetail = {
    val split = str.split(MetaUtils.LAKE_SOUL_SEP_03, -1)
    assert(split.length == 5)

    val leftTables = if (split(0).equals("")) {
      Set.empty[String]
    } else {
      split(0).split(MetaUtils.LAKE_SOUL_SEP_02).toSet
    }
    val rightTables = if (split(1).equals("")) {
      Set.empty[String]
    } else {
      split(1).split(MetaUtils.LAKE_SOUL_SEP_02).toSet
    }
    val equal = if (split(3).equals("")) {
      Set.empty[(String, String)]
    } else {
      split(3).split(MetaUtils.LAKE_SOUL_SEP_02, -1).map(m => {
        val arr = m.split(MetaUtils.LAKE_SOUL_SEP_01)
        assert(arr.length == 2)
        (arr(0), arr(1))
      }).toSet
    }
    val other = if (split(4).equals("")) {
      Set.empty[String]
    } else {
      split(4).split(MetaUtils.LAKE_SOUL_SEP_02).toSet
    }

    JoinDetail(
      leftTables,
      rightTables,
      split(2),
      equal,
      other)
  }
}


case class JoinDetail(leftTables: Set[String],
                      rightTables: Set[String],
                      joinType: String,
                      equalCondition: Set[(String, String)],
                      otherCondition: Set[String]) {
  override def toString: String = {
    val equal = equalCondition
      .map(m => m._1 + MetaUtils.LAKE_SOUL_SEP_01 + m._2)
      .mkString(MetaUtils.LAKE_SOUL_SEP_02)
    val other = otherCondition.mkString(MetaUtils.LAKE_SOUL_SEP_02)

    Seq(leftTables.mkString(MetaUtils.LAKE_SOUL_SEP_02),
      rightTables.mkString(MetaUtils.LAKE_SOUL_SEP_02),
      joinType,
      equal,
      other)
      .mkString(MetaUtils.LAKE_SOUL_SEP_03)
  }
}