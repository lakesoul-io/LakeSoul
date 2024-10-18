// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{CallArgument, NamedArgument}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.{IntegerType, LongType}

case class CallExecCommand(action: String, args: Seq[CallArgument]) extends LeafRunnableCommand {
  private val tableName = "tablename"
  private val tableNamespace = "tablenamespace"
  private val tablePath = "tablepath"
  private val parValue = "partitionvalue"
  private val toTime = "totime"
  private val zoneId = "zoneid"
  private val hiveTableName = "hivetablename"
  private val cleanOld = "cleanold"
  private val condition = "condition"
  private val toVersion = "toversion"


  override def run(sparkSession: SparkSession): Seq[Row] = {
    val argsMap = scala.collection.mutable.HashMap[String, Expression]()
    for (item <- args) {
      if (item.isInstanceOf[NamedArgument]) {
        val argument = item.asInstanceOf[NamedArgument]
        argsMap += argument.name.toLowerCase() -> argument.expr
      }
    }
    if (argsMap.size == 0) {
      throw new AnalysisException("no arguments")
    }

    action match {
      case "rollback" => {
        checkRollbackArgs(args)
        val table = getLakeSoulTable(argsMap)
        if (argsMap.contains(toVersion)) {
          table.rollbackPartition(getPartitionVal(argsMap(parValue)), argsMap(toVersion).toString().toInt)
        } else if (argsMap.contains(zoneId)) {
          table.rollbackPartition(getPartitionVal(argsMap(parValue)), argsMap(toTime).toString(), argsMap(zoneId).toString())
        } else {
          table.rollbackPartition(getPartitionVal(argsMap(parValue)), argsMap(toTime).toString())
        }
      }
      case "compaction" => {
        checkCompactionArgs(args)
        val table = getLakeSoulTable(argsMap)
        val conditons = if (argsMap.contains(condition)) {
          getContition(argsMap(condition))
        } else {
          ""
        }
        if (argsMap.contains(hiveTableName)) {
          table.compaction(conditons, hiveTableName = argsMap(hiveTableName).toString())
        }
        if (argsMap.contains(cleanOld)) {
          table.compaction(cleanOldCompaction = argsMap(cleanOld).toString().toBoolean)
        }
        table.compaction(conditons)
      }
    }
    Seq.empty
  }

  private def getLakeSoulTable(argsMap: scala.collection.mutable.HashMap[String, Expression]): LakeSoulTable = {
    if (argsMap.contains(tableName)) {
      val tName = argsMap(tableName).toString()
      val tNameSpace = if (argsMap.contains(tableNamespace)) {
        argsMap(tableNamespace).toString()
      } else {
        ""
      }
      if (tNameSpace.equals("")) {
        toLakeSoulDFFromName(tName)
      } else {
        toLakeSoulDFFromNameAndNamespace(tName, tNameSpace)
      }
    } else {
      val tPath = argsMap(tablePath).toString()
      toLakeSoulDFFromPath(tPath)
    }
  }

  private def getContition(contitons: Expression): String = {
    if (contitons.isInstanceOf[Literal]) {
      return contitons.asInstanceOf[Literal].toString()
    }
    if (!contitons.isInstanceOf[UnresolvedFunction]) {
      throw new AnalysisException("conditions not support;for example condition='' or condition=map(a=>1,c=>'d')")
    }
    val parType = contitons.asInstanceOf[UnresolvedFunction].nameParts(0)
    if (!"map".equalsIgnoreCase(parType)) {
      throw new AnalysisException("condititon type is not map; for example map(a=>1,c=>'d')")
    }
    val parValue = contitons.asInstanceOf[UnresolvedFunction].arguments
    if (parValue.size == 0) {
      return ""
    }
    val content: StringBuilder = new StringBuilder()
    for (i <- (1 to parValue.size by 2)) {
      val parName = parValue(i - 1)
      val parVal = parValue(i)
      if (parVal.dataType.isInstanceOf[IntegerType] || parVal.dataType.isInstanceOf[LongType]) {
        content.append(parName.toString() + "=" + parVal.toString() + " and ")
      } else {
        content.append(parName.toString() + "='" + parVal.toString() + "' and ")
      }
    }
    content.toString().substring(0, content.toString().length - 5)
  }


  private def getPartitionVal(partition: Expression): String = {
    val parType = partition.asInstanceOf[UnresolvedFunction].nameParts(0)
    if (!"map".equalsIgnoreCase(parType)) {
      throw new AnalysisException("partition type not map; for example map(a=>1,c=>'d')")
    }
    val parValue = partition.asInstanceOf[UnresolvedFunction].arguments
    if (parValue.size == 2) {
      if ("-5".equals(parValue(0).toString())) {
        return "-5"
      }
    }
    val content: StringBuilder = new StringBuilder()
    for (i <- (1 to parValue.size by 2)) {
      val parName = parValue(i - 1)
      val parVal = parValue(i)
      if (parVal.dataType.isInstanceOf[IntegerType] || parVal.dataType.isInstanceOf[LongType]) {
        content.append(parName.toString() + "=" + parVal.toString() + ",")
      } else {
        content.append(parName.toString() + "='" + parVal.toString() + "',")
      }
    }
    content.toString().substring(0, content.toString().length - 1)
  }

  private def toLakeSoulDFFromPath(tablePath: String): LakeSoulTable = {
    LakeSoulTable.forPath(tablePath)
  }

  private def toLakeSoulDFFromName(tableName: String): LakeSoulTable = {
    LakeSoulTable.forName(tableName)
  }

  private def toLakeSoulDFFromNameAndNamespace(tableName: String, nameSpace: String): LakeSoulTable = {
    LakeSoulTable.forName(tableName, nameSpace)
  }

  private def checkRollbackArgs(rollbackArgs: Seq[CallArgument]): Boolean = {
    val names = rollbackArgs.map(arg => if (arg.isInstanceOf[NamedArgument]) {
      arg.asInstanceOf[NamedArgument].name.toLowerCase()
    })
    if (names.size > 0) {
      if ((names.contains(tableName) || names.contains(tablePath)) && names.contains(parValue) && (names.contains(toTime) || names.contains(toVersion))) {
        true
      } else {
        false
      }
    } else {
      throw new AnalysisException("no rollback arguments")
    }

  }

  private def checkCompactionArgs(compactionArgs: Seq[CallArgument]): Boolean = {
    val names = compactionArgs.map(arg => if (arg.isInstanceOf[NamedArgument]) {
      arg.asInstanceOf[NamedArgument].name.toLowerCase()
    })
    if (names.size > 0) {
      if ((names.contains(tablePath) || names.contains(tableName))) {
        true
      } else {
        false
      }
    } else {
      throw new AnalysisException("no rollback arguments")
    }
  }
}