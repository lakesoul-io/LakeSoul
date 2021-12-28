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

import java.util
import com.alibaba.fastjson.JSON
import com.dmetasoul.lakesoul.meta.MetaUtils
import org.apache.spark.sql.lakesoul.material_view.ConstructQueryInfo.getFinalStringByReplace
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class OrInfo extends ConstructProperties {
  private val rangeInfo: mutable.Map[String, RangeInfo] = mutable.Map[String, RangeInfo]()
  private val conditionEqualInfo: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()
  //condition may has nest `or` expression, like a.k=1 or (a.k=2 and (a.value>5 or a.value<0))
  private val conditionOrInfo: ArrayBuffer[OrInfo] = new ArrayBuffer[OrInfo]()
  private val otherInfo: ArrayBuffer[String] = new ArrayBuffer[String]()

  override def addRangeInfo(dataType: DataType, colName: String, limit: Any, rangeType: String): Unit = {
    RangeInfo.setRangeInfo(rangeInfo, dataType, colName, limit, rangeType)
  }

  override def addConditionEqualInfo(left: String, right: String): Unit = {
    conditionEqualInfo += ((left, right))
  }

  override def addConditionOrInfo(orInfo: OrInfo): Unit = {
    conditionOrInfo += orInfo
  }

  override def addOtherInfo(info: String): Unit = {
    otherInfo += info
  }

  override def addColumnEqualInfo(left: String, right: String): Unit = {}

  def buildDetail(tables: Map[String, String], asInfo: Map[String, String]): OrDetail = {
    val rangeInfoNew = rangeInfo.map(m => {
      getFinalStringByReplace(m._1, tables, asInfo) -> m._2.buildDetail()
    }).toMap

    val conditionEqualInfoNew = conditionEqualInfo
      .map(m => getFinalStringByReplace(m._1, tables, asInfo) -> m._2)
      .toMap

    val conditionOrInfoNew = conditionOrInfo.map(_.buildDetail(tables, asInfo))

    val otherInfoNew = otherInfo.map(m => getFinalStringByReplace(m, tables, asInfo))

    OrDetail(
      rangeInfoNew,
      conditionEqualInfoNew,
      conditionOrInfoNew,
      otherInfoNew
    )
  }


}


object OrInfo {
  //return true if query's OrInfo is in scope of view's
  def inbounds(query: Seq[OrDetail], view: Seq[OrDetail]): Boolean = {
    if (view.isEmpty) {
      true
    } else {
      if (query.isEmpty) {
        false
      } else {
        query.forall(qod => {
          view.exists(vod => {
            //otherInfo inbounds
            var matching = vod.otherInfo.diff(qod.otherInfo).isEmpty

            //conditionEqualInfo inbounds
            if (matching) {
              val diff = vod.conditionEqualInfo.map(m => m._1 + "=" + m._2).toSeq
                .diff(qod.conditionEqualInfo.map(m => m._1 + "=" + m._2).toSeq)
              if (diff.nonEmpty) {
                matching = false
              }
            }

            //rangeInfo of view should cover query
            if (matching) {
              matching = vod.rangeInfo.forall(vr => {
                var flag = true
                if (qod.rangeInfo.contains(vr._1)) {
                  val compare = RangeInfo.compareRangeDetail(qod.rangeInfo(vr._1), vr._2)
                  if (compare < 0) {
                    flag = false
                  }
                } else if (qod.conditionEqualInfo.contains(vr._1)) {
                  val value = qod.conditionEqualInfo(vr._1)
                  if (!RangeInfo.valueInRange(value, vr._2)) {
                    flag = false
                  }
                } else {
                  flag = false
                }
                flag
              })
            }

            //orInfo
            if (matching) {
              val childQueryOrInfo =
                if (qod.conditionOrInfo.isEmpty && vod.conditionOrInfo.nonEmpty) {
                  Seq(OrDetail(
                    qod.rangeInfo,
                    qod.conditionEqualInfo,
                    qod.conditionOrInfo,
                    qod.otherInfo))
                } else {
                  qod.conditionOrInfo
                }
              matching = inbounds(childQueryOrInfo, vod.conditionOrInfo)
            }

            matching
          })

        })
      }

    }
  }

  def buildDetail(jsonString: String): OrDetail = {
    val jsonObj = JSON.parseObject(jsonString.replace(MetaUtils.LAKESOUL_META_QUOTE, "'"))


    val rangeJson = JSON.parseObject(jsonObj.getString("rangeInfo"))
    val rangeInfo = rangeJson.getInnerMap.asScala.map(m => {
      val detail = RangeInfo.buildDetail(m._2.toString)
      m._1 -> detail
    }).toMap

    val conditionEqualJson = JSON.parseObject(jsonObj.getString("conditionEqualInfo"))
    val conditionEqualInfo = conditionEqualJson.getInnerMap.asScala.map(m => m._1 -> m._2.toString).toMap

    val conditionOrJson = JSON.parseObject(jsonObj.getString("conditionOrInfo"))
    val conditionOrInfo = conditionOrJson.getInnerMap.asScala.map(m => buildDetail(m._2.toString)).toSeq


    val otherInfoString = jsonObj.getString("otherInfo")
    val otherInfo = if (otherInfoString.equals("")) {
      Seq.empty[String]
    } else {
      otherInfoString.split(MetaUtils.LAKE_SOUL_SEP_01).toSeq
    }

    OrDetail(rangeInfo, conditionEqualInfo, conditionOrInfo, otherInfo)

  }


}


case class OrDetail(rangeInfo: Map[String, RangeDetail],
                    conditionEqualInfo: Map[String, String],
                    conditionOrInfo: Seq[OrDetail],
                    otherInfo: Seq[String]) {
  override def toString: String = {
    //build json
    val jsonMap = new util.HashMap[String, String]()

    val rangeMap = new util.HashMap[String, String]()
    rangeInfo.foreach(info => {
      rangeMap.put(info._1, info._2.toString)
    })
    jsonMap.put("rangeInfo", JSON.toJSON(rangeMap).toString)

    val conditionEqualMap = new util.HashMap[String, String]()
    conditionEqualInfo.foreach(info => {
      conditionEqualMap.put(info._1, info._2)
    })
    jsonMap.put("conditionEqualInfo", JSON.toJSON(conditionEqualMap).toString)


    val conditionOrMap = new util.HashMap[String, String]()
    conditionOrInfo.map(_.toString).zipWithIndex.foreach(info => {
      conditionOrMap.put(info._2.toString, info._1)
    })
    jsonMap.put("conditionOrInfo", JSON.toJSON(conditionOrMap).toString)


    jsonMap.put("otherInfo", otherInfo.mkString(MetaUtils.LAKE_SOUL_SEP_01))


    //trans all info to json and replace quote
    var json = JSON.toJSON(jsonMap).toString
    json = json.replace("'", MetaUtils.LAKESOUL_META_QUOTE)

    json

  }

}
