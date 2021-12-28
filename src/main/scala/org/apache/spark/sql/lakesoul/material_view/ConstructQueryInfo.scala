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

import com.alibaba.fastjson.JSON
import com.dmetasoul.lakesoul.meta.MetaUtils
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.lakesoul.material_view.ConstructQueryInfo._
import org.apache.spark.sql.lakesoul.material_view.OrInfo.buildDetail
import org.apache.spark.sql.types.DataType

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ConstructQueryInfo extends ConstructProperties {
  private var firstConstruct: Boolean = true

  private val outputInfo: mutable.Map[String, String] = mutable.Map[String, String]()

  //equal info of different columns, such as a.key=b.key
  private val columnEqualInfo: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()

  //equal info of conditions, such as: a.key=1
  private val conditionEqualInfo: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()

  //or info of conditions, such as: a.key=1 or (a.key=2 and a.value>5)
  private val conditionOrInfo: ArrayBuffer[OrInfo] = new ArrayBuffer[OrInfo]()

  private val tableInfo: mutable.Map[String, String] = mutable.Map[String, String]()
  private val columnAsInfo: mutable.Map[String, String] = mutable.Map[String, String]()
  private val rangeInfo: mutable.Map[String, RangeInfo] = mutable.Map[String, RangeInfo]()
  private val otherInfo: ArrayBuffer[String] = new ArrayBuffer[String]()

  private val joinInfo: ArrayBuffer[JoinDetail] = new ArrayBuffer[JoinDetail]()
  private val aggregateInfo: AggregateInfo = new AggregateInfo()


  def addJoinInfo(info: JoinDetail): Unit = {
    joinInfo += info
  }

  def addOutputInfo(name: String, referenceName: String): Unit = {
    val re = outputInfo.put(name, referenceName)
    assert(re.isEmpty, s"outputInfo map already has key($name), " +
      s"exists value(${re.get}), new value($referenceName)")
  }


  def addTableInfo(name: String, referenceName: String): Unit = {
    val re = tableInfo.put(name, referenceName)
    assert(re.isEmpty, s"tableInfo map already has key($name), " +
      s"exists value(${re.get}), new value($referenceName)")
  }

  def addColumnAsInfo(name: String, referenceName: String): Unit = {
    if (!referenceName.replace("`", "").equals(name)) {
      val re = columnAsInfo.put(name, referenceName)
      assert(re.isEmpty, s"columnAsInfo map already has key($name), " +
        s"exists value(${re.get}), new value($referenceName)")
    }
  }


  def addOtherInfo(info: String): Unit = {
    otherInfo += info
  }


  def addColumnEqualInfo(left: String, right: String): Unit = {
    columnEqualInfo += ((left, right))
  }

  def addConditionEqualInfo(left: String, right: String): Unit = {
    conditionEqualInfo += ((left, right))
  }

  def addConditionOrInfo(orInfo: OrInfo): Unit = {
    conditionOrInfo += orInfo
  }


  def addRangeInfo(dataType: DataType, colName: String, limit: Any, rangeType: String): Unit = {
    RangeInfo.setRangeInfo(rangeInfo, dataType, colName, limit, rangeType)
  }

  def setAggEqualCondition(left: String, right: String): Unit = {
    aggregateInfo.setAggEqualCondition(left, right)
  }

  def setAggOtherCondition(cond: String): Unit = {
    aggregateInfo.setAggOtherCondition(cond)
  }

  def setAggInfo(tables: Set[String], cols: Set[String]): Unit = {
    aggregateInfo.setAggInfo(tables, cols)
  }


  def buildQueryInfo(): QueryInfo = {
    assert(firstConstruct, "It has been built before, you can't build query info more than once")
    firstConstruct = false

    //matching final lakesoul table
    val tables = tableInfo.keys
      .filter(f => !f.startsWith("lakesoul.`"))
      .map(key => {
        var flag = true
        var value = tableInfo(key)
        while (flag) {
          value = tableInfo(value)
          if (value.startsWith("lakesoul.`")) {
            flag = false
          }
        }
        (key, value)
      }).toMap

    //replace table to final lakesoul table
    val columnAsInfoTmp = columnAsInfo.map(m => {
      m._1 -> replaceByTableInfo(tables, m._2)
    }).filter(m => !m._1.equals(m._2)).toMap

    //replace column as condition to final lakesoul table column
    val columnAsInfoNew = columnAsInfoTmp.map(m => {
      var flag = true
      var value = m._2
      while (flag) {
        if (columnAsInfoTmp.contains(value)) {
          value = columnAsInfoTmp(value)
        } else {
          flag = false
        }
      }
      m._1 -> value
    })

    val outputInfoNew = outputInfo.map(m => {
      //replace temp table to final lakesoul table
      val formatName = replaceByTableInfo(tables, m._2)

      val finalName = if (columnAsInfoNew.contains(formatName)) {
        //replace alias fields to final lakesoul table fields
        columnAsInfoNew(formatName)
      } else {
        formatName
      }

      m._1 -> finalName
    }).toMap


    val columnEqualInfoTmp = columnEqualInfo.map(m => {
      val key = getFinalStringByReplace(m._1, tables, columnAsInfoNew)
      val value = getFinalStringByReplace(m._2, tables, columnAsInfoNew)
      (key, value)
    })

    val columnEqualInfoNew: ArrayBuffer[mutable.Set[String]] = new ArrayBuffer[mutable.Set[String]]()

    for (info <- columnEqualInfoTmp) {
      val find = columnEqualInfoNew.find(f => f.contains(info._1) || f.contains(info._2))
      if (find.isDefined) {
        find.get.add(info._1)
        find.get.add(info._2)
      } else {
        val set = mutable.Set[String]()
        set.add(info._1)
        set.add(info._2)
        columnEqualInfoNew += set
      }
    }


    val rangeInfoNew = rangeInfo.map(m => {
      getFinalStringByReplace(m._1, tables, columnAsInfoNew) -> m._2.buildDetail()
    }).toMap


    val joinInfoNew = if (joinInfo.isEmpty) {
      JoinInfo.buildEmptyInfo()
    } else if (joinInfo.head.joinType.equals(Inner.sql)) {
      assert(joinInfo.forall(f => Inner.sql.equals(f.joinType)), "Multi table join can only used with inner join")
      assert(joinInfo.forall(f => f.otherCondition.isEmpty && f.equalCondition.isEmpty),
        "Inner join condition should be extract with where condition")

      //transform tables to final lakesoul table
      val left = joinInfo
        .flatMap(m => m.leftTables ++ m.rightTables)
        .map(m => {
          if (m.startsWith("lakesoul.`")) {
            m
          } else {
            tables(m)
          }
        })
        .toSet
      JoinDetail(
        left,
        Set.empty[String],
        Inner.sql,
        Set.empty[(String, String)],
        Set.empty[String])
    } else {
      assert(joinInfo.length == 1, "Multi table join can only used with inner join")

      //transform tables to final lakesoul table
      val left = joinInfo.head.leftTables.map(m => {
        if (m.startsWith("lakesoul.`")) {
          m
        } else {
          tables(m)
        }
      })
      val right = joinInfo.head.rightTables.map(m => {
        if (m.startsWith("lakesoul.`")) {
          m
        } else {
          tables(m)
        }
      })
      val equalCondition = joinInfo.head.equalCondition.map(cond => {
        val key = getFinalStringByReplace(cond._1, tables, columnAsInfoNew)
        val value = getFinalStringByReplace(cond._2, tables, columnAsInfoNew)
        (key, value)
      })
      val otherCondition = joinInfo.head.otherCondition.map(m =>
        getFinalStringByReplace(m, tables, columnAsInfoNew))

      JoinDetail(
        left,
        right,
        joinInfo.head.joinType,
        equalCondition,
        otherCondition
      )

    }


    val aggregateInfoNew = aggregateInfo.buildAggregateDetail(tables, columnAsInfoNew)

    val conditionEqualInfoNew = conditionEqualInfo.map(m => {
      getFinalStringByReplace(m._1, tables, columnAsInfoNew) -> m._2
    }).toMap

    val conditionOrInfoNew = conditionOrInfo.map(_.buildDetail(tables, columnAsInfoNew))

    val otherInfoNew = otherInfo.map(m => getFinalStringByReplace(m, tables, columnAsInfoNew))

    QueryInfo(
      outputInfoNew,
      columnEqualInfoNew.map(m => m.toSet),
      columnAsInfoNew,
      rangeInfoNew,
      joinInfoNew,
      aggregateInfoNew,
      conditionEqualInfoNew,
      conditionOrInfoNew,
      otherInfoNew,
      tables)

  }


}

object ConstructQueryInfo {

  //replace the prefix of fields to final lakesoul table name
  def replaceByTableInfo(allTables: Map[String, String], str: String): String = {
    var result = str
    allTables.foreach(t => {
      result = result.replace(s"${t._1}.`", s"${t._2}.`")
    })
    result
  }

  //replace alias fields to final lakesoul table fields
  def replaceByColumnAsInfo(asInfo: Map[String, String], str: String): String = {
    var result = str
    asInfo.foreach(t => {
      result = result.replace(t._1, t._2)
      //eg: use Map(`key` -> lakesoul.`t1`) to replace string "concat_ws(',',`key`,a.`key`)"
      //result will be "concat_ws(',',lakesoul.`t1`,a.lakesoul.`t1`)", it is not expect result,
      //use another Map(.lakesoul.`t1` -> .`key`) to get the expect result "concat_ws(',',lakesoul.`t1`,a.`key`)"
      result = result.replace(s".${t._2}", s".${t._1}")
    })
    result
  }

  def getFinalStringByReplace(str: String,
                              allTables: Map[String, String],
                              asInfo: Map[String, String]): String = {
    replaceByColumnAsInfo(asInfo, replaceByTableInfo(allTables, str))
  }

  def buildJson(info: QueryInfo): String = {
    //build json
    val jsonMap = new util.HashMap[String, String]()

    val outputMap = new util.HashMap[String, String]()
    info.outputInfo.foreach(info => {
      outputMap.put(info._1, info._2)
    })
    jsonMap.put("outputInfo", JSON.toJSON(outputMap).toString)


    val columnEqualInfoString = info.columnEqualInfo
      .map(m => m.mkString(MetaUtils.LAKE_SOUL_SEP_01))
      .mkString(MetaUtils.LAKE_SOUL_SEP_02)
    jsonMap.put("columnEqualInfo", columnEqualInfoString)

    val columnAsMap = new util.HashMap[String, String]()
    info.columnAsInfo.foreach(info => {
      columnAsMap.put(info._1, info._2)
    })
    jsonMap.put("columnAsInfo", JSON.toJSON(columnAsMap).toString)

    val rangeMap = new util.HashMap[String, String]()
    info.rangeInfo.foreach(info => {
      rangeMap.put(info._1, info._2.toString)
    })
    jsonMap.put("rangeInfo", JSON.toJSON(rangeMap).toString)

    jsonMap.put("joinInfo", info.joinInfo.toString)

    jsonMap.put("aggregateInfo", info.aggregateInfo.toString)


    val conditionEqualMap = new util.HashMap[String, String]()
    info.conditionEqualInfo.foreach(info => {
      conditionEqualMap.put(info._1, info._2)
    })
    jsonMap.put("conditionEqualInfo", JSON.toJSON(conditionEqualMap).toString)

    val conditionOrMap = new util.HashMap[String, String]()
    info.conditionOrInfo.map(_.toString).zipWithIndex.foreach(info => {
      conditionOrMap.put(info._2.toString, info._1)
    })
    jsonMap.put("conditionOrInfo", JSON.toJSON(conditionOrMap).toString)


    jsonMap.put("otherInfo", info.otherInfo.mkString(MetaUtils.LAKE_SOUL_SEP_01))


    //trans all info to json and replace quote
    var json = JSON.toJSON(jsonMap).toString
    json = json.replace("'", MetaUtils.LAKESOUL_META_QUOTE)

    json
  }


  //build info from json string which got from meta data
  def buildInfo(jsonString: String): QueryInfo = {
    val jsonObj = JSON.parseObject(jsonString.replace(MetaUtils.LAKESOUL_META_QUOTE, "'"))

    val outputJson = JSON.parseObject(jsonObj.getString("outputInfo"))
    val outputInfo = outputJson.getInnerMap.asScala.map(m => m._1 -> m._2.toString).toMap

    val columnEqualInfoString = jsonObj.getString("columnEqualInfo")
    val columnEqualInfo = if (columnEqualInfoString.equals("")) {
      Seq.empty[Set[String]]
    } else {
      columnEqualInfoString
        .split(MetaUtils.LAKE_SOUL_SEP_02)
        .map(m => m.split(MetaUtils.LAKE_SOUL_SEP_01).toSet)
        .toSeq
    }

    val columnAsJson = JSON.parseObject(jsonObj.getString("columnAsInfo"))
    val columnAsInfo = columnAsJson.getInnerMap.asScala.map(m => m._1 -> m._2.toString).toMap

    val rangeJson = JSON.parseObject(jsonObj.getString("rangeInfo"))
    val rangeInfo = rangeJson.getInnerMap.asScala.map(m => {
      val detail = RangeInfo.buildDetail(m._2.toString)
      m._1 -> detail
    }).toMap

    val joinJson = jsonObj.getString("joinInfo")
    val joinInfo = JoinInfo.build(joinJson)

    val aggregateJson = jsonObj.getString("aggregateInfo")
    val aggregateInfo = AggregateInfo.buildDetail(aggregateJson)


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


    val tableInfo = Map.empty[String, String]
    QueryInfo(
      outputInfo,
      columnEqualInfo,
      columnAsInfo,
      rangeInfo,
      joinInfo,
      aggregateInfo,
      conditionEqualInfo,
      conditionOrInfo,
      otherInfo,
      tableInfo)


  }


}

//all query/table info
case class QueryInfo(outputInfo: Map[String, String],
                     columnEqualInfo: Seq[Set[String]],
                     columnAsInfo: Map[String, String],
                     rangeInfo: Map[String, RangeDetail],
                     joinInfo: JoinDetail,
                     aggregateInfo: AggregateDetail,
                     conditionEqualInfo: Map[String, String],
                     conditionOrInfo: Seq[OrDetail],
                     otherInfo: Seq[String],
                     tableInfo: Map[String, String])
