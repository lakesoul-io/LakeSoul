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

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeserializeToObject, Filter, Join, LocalRelation, LogicalPlan, Project, Range, SerializeFromObject, SubqueryAlias}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, FileScan}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.RelationTable
import org.apache.spark.sql.lakesoul.{PartitionFilter, LakeSoulFileIndexV2}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object MaterialViewUtils extends PredicateHelper {

  def parseOutputInfo(plan: LogicalPlan, constructInfo: ConstructQueryInfo): Unit = {
    plan match {
      case Project(projectList, child) =>
        projectList.foreach({
          case attr: AttributeReference =>
            constructInfo.addOutputInfo(attr.qualifiedName, attr.sql)

          case as: Alias => constructInfo.addOutputInfo(as.qualifiedName, as.child.sql)
        })

      //fast failed when parsing Aggregate with havingCondition, because it is not a real plan
      case Aggregate(_, aggExpr, _) if aggExpr.head.isInstanceOf[Alias]
        && aggExpr.head.asInstanceOf[Alias].name.equals("havingCondition") =>
        throw LakeSoulErrors.unsupportedLogicalPlanWhileRewriteQueryException("havingCondition")

      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        aggregateExpressions.foreach {
          case attr: AttributeReference =>
            constructInfo.addOutputInfo(attr.qualifiedName, attr.sql)

          case as: Alias => constructInfo.addOutputInfo(as.qualifiedName, as.child.sql)
        }

      //fast failed when parsing unsupported query
      case a: LogicalPlan => throw LakeSoulErrors.unsupportedLogicalPlanWhileRewriteQueryException(a.toString())

    }
  }

  def parseMaterialInfo(plan: LogicalPlan,
                        constructInfo: ConstructQueryInfo,
                        underAggregate: Boolean): Unit = {

    def findTableNames(source: LogicalPlan, set: mutable.Set[String], allowJoin: Boolean): Unit = {
      source match {
        case _: Aggregate =>
          throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
            "Aggregate can't exist in Join expression and Aggregate expression")

        case Join(_, _, joinType, _, _) if !(allowJoin || joinType.sql.equals(Inner.sql)) =>
          throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
            "Multi table join can only used with inner join")

        case DataSourceV2Relation(table, _, _, _, _) if !table.isInstanceOf[LakeSoulTableV2] =>
          throw LakeSoulErrors.materialViewBuildWithNonLakeSoulTableException()

        case DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) => set.add(table.name())

        //Recursive search table name
        case o => o.children.foreach(findTableNames(_, set, allowJoin))

      }
    }


    plan match {
      //fast failed
      case e: Range =>
        throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(s"unsupport plan ${e.toString()}")

      case e: SerializeFromObject =>
        throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(s"unsupport plan ${e.toString()}")

      case e: DeserializeToObject =>
        throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(s"unsupport plan ${e.toString()}")

      case e: LocalRelation =>
        throw LakeSoulErrors.materialViewBuildWithNonLakeSoulTableException()

      case Project(projectList, child) =>
        projectList.foreach({
          case as: Alias => constructInfo.addColumnAsInfo("`" + as.name + "`", as.child.sql)
          case _ =>
        })
        parseMaterialInfo(child, constructInfo, underAggregate)

      case Filter(condition, child) =>
        val withoutAliasCondition = condition match {
          case Alias(c, _) => c
          case other => other
        }
        val filters = splitConjunctivePredicates(withoutAliasCondition)
        //if this filter is under Aggregate, it should match all with query
        if (underAggregate) {
          filters.foreach {
            case EqualTo(left, right) =>
              constructInfo.addColumnEqualInfo(left.sql, right.sql)
              constructInfo.setAggEqualCondition(left.sql, right.sql)
            case other => constructInfo.setAggOtherCondition(other.sql)
          }

          parseMaterialInfo(child, constructInfo, underAggregate)
        } else {
          //if not under Aggregate, we can parse them to range conditions
          filters.foreach(f => parseCondition(f, constructInfo))
          parseMaterialInfo(child, constructInfo, underAggregate)
        }


      case Join(left, right, joinType, condition, hint) =>
        val joinInfo = new JoinInfo()
        val leftTables = mutable.Set[String]()
        val rightTables = mutable.Set[String]()

        //Multi table join can only used with inner join
        val allowJoinBelow = joinType.sql.equals(Inner.sql)
        findTableNames(left, leftTables, allowJoinBelow)
        findTableNames(right, rightTables, allowJoinBelow)
        joinInfo.setJoinInfo(leftTables.toSet, rightTables.toSet, joinType.sql)

        if (condition.isDefined) {
          if (allowJoinBelow) {
            //inner join condition can be extract to `where`
            //sql: select * from a join b on (a.id=b.id and a.v>1) where b.v>2
            // is semantically equivalent to
            //sql: select * from a join b where a.id=b.id and a.v>1 and b.v>2
            if (underAggregate) {
              //add condition to aggInfo which should match all condition when rewrite
              splitConjunctivePredicates(condition.get).foreach {
                case EqualTo(l, r) =>
                  constructInfo.addColumnEqualInfo(l.sql, r.sql)
                  constructInfo.setAggEqualCondition(l.sql, r.sql)
                case other => constructInfo.setAggOtherCondition(other.sql)
              }
            } else {
              //add condition to query level info which can match part of condition when rewrite
              splitConjunctivePredicates(condition.get).foreach(f => parseCondition(f, constructInfo))
            }
          } else {
            //add condition to join info
            splitConjunctivePredicates(condition.get).foreach {
              case EqualTo(left: AttributeReference, right: AttributeReference) =>
                constructInfo.addColumnEqualInfo(left.sql, right.sql)
                joinInfo.setJoinEqualCondition(left.sql, right.sql)

              case other => joinInfo.setJoinOtherCondition(other.sql)
            }
          }
        }


        val joinDetail = joinInfo.buildJoinDetail()
        constructInfo.addJoinInfo(joinDetail)
        parseMaterialInfo(left, constructInfo, underAggregate)
        parseMaterialInfo(right, constructInfo, underAggregate)


      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
        if (underAggregate) {
          throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
            "Multi aggregate expression is not support now")
        }
        //set aggregate info
        val groupCols = groupingExpressions.map(f => f.sql)
        val aggTables = mutable.Set[String]()
        findTableNames(child, aggTables, true)
        constructInfo.setAggInfo(aggTables.toSet, groupCols.toSet)

        //set alias info
        aggregateExpressions.foreach({
          case as: Alias => constructInfo.addColumnAsInfo("`" + as.name + "`", as.child.sql)
          case _ =>
        })

        parseMaterialInfo(child, constructInfo, true)


      case SubqueryAlias(ident, child) =>
        val prefix = ident.toString() + ".`"
        child match {
          case Project(projectList, lowerChild) =>
            projectList.foreach({
              case attr: AttributeReference =>
                constructInfo.addColumnAsInfo(prefix + attr.name + "`", attr.sql)

              case as: Alias =>
                constructInfo.addColumnAsInfo(prefix + as.name + "`", as.child.sql)
            })
            parseMaterialInfo(lowerChild, constructInfo, underAggregate)

          case SubqueryAlias(identifier, _) =>
            constructInfo.addTableInfo(ident.toString(), identifier.toString())
            parseMaterialInfo(child, constructInfo, underAggregate)

          case DataSourceV2Relation(table: LakeSoulTableV2, _, _, identifier, _) =>
            constructInfo.addTableInfo(ident.toString(), table.name())
            parseMaterialInfo(child, constructInfo, underAggregate)

          //
          case a: LogicalPlan => throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
            "unsupported child plan when parse SubqueryAlias")

        }


      case DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) =>
        //todo
//        if (table.snapshotManagement.getTableInfoOnly.is_material_view) {
//          throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
//            "A material view can't be used to create or rewrite another material view")
//        }
        constructInfo.addTableInfo(table.name(), s"lakesoul.`${table.snapshotManagement.table_name}`")

      case DataSourceV2Relation(table, _, _, _, _) if !table.isInstanceOf[LakeSoulTableV2] =>
        throw LakeSoulErrors.materialViewBuildWithNonLakeSoulTableException()


      case lp: LogicalPlan if lp.children.nonEmpty =>
        lp.children.foreach(f => parseMaterialInfo(f, constructInfo, underAggregate))


      case _ =>

    }
  }


  //parse conditions which can
  private def parseCondition(condition: Expression, constructInfo: ConstructProperties): Unit = {
    condition match {
      case expression: EqualTo =>
        parseEqualCondition(expression, constructInfo)

      case expression: EqualNullSafe =>
        parseEqualCondition(expression, constructInfo)

      case expression: GreaterThan =>
        parseRangeCondition(expression, constructInfo)

      case expression: GreaterThanOrEqual =>
        parseRangeCondition(expression, constructInfo)

      case expression: LessThan =>
        parseRangeCondition(expression, constructInfo)

      case expression: LessThanOrEqual =>
        parseRangeCondition(expression, constructInfo)


      case other =>
        val splitOr = splitDisjunctivePredicates(other)
        if (splitOr.length == 1) {
          constructInfo.addOtherInfo(other.sql)
        } else {
          //there are some `or` conditions in this expression
          splitOr.foreach(f => {
            val orInfo = new OrInfo()
            //split expression by `And`, then parse them
            splitConjunctivePredicates(f).foreach(e => parseCondition(e, orInfo))

            constructInfo.addConditionOrInfo(orInfo)
          })

        }


    }
  }

  private def parseEqualCondition(condition: Expression, info: ConstructProperties): Unit = {
    condition match {
      case EqualTo(left, right: Literal) =>
        info.addConditionEqualInfo(left.sql, right.value.toString)

      case EqualTo(left: Literal, right) =>
        info.addConditionEqualInfo(right.sql, left.value.toString)

      case e@EqualTo(left, right) =>
        info.addColumnEqualInfo(left.sql, right.sql)
        //sort to match equal condition, like (t1.a=t2.a) compare to (t2.a=t1.a)
        if (left.sql.compareTo(right.sql) <= 0) {
          info.addOtherInfo(e.sql)
        } else {
          info.addOtherInfo(e.copy(left = right, right = left).sql)
        }

      case EqualNullSafe(left, right: Literal) =>
        info.addConditionEqualInfo(left.sql, right.value.toString)

      case EqualNullSafe(left: Literal, right) =>
        info.addConditionEqualInfo(right.sql, left.value.toString)

      case e@EqualNullSafe(left, right) =>
        info.addColumnEqualInfo(left.sql, right.sql)
        //sort to match equal condition, like (t1.a=t2.a) compare to (t2.a=t1.a)
        if (left.sql.compareTo(right.sql) <= 0) {
          info.addOtherInfo(e.sql)
        } else {
          info.addOtherInfo(e.copy(left = right, right = left).sql)
        }

    }
  }

  private def parseRangeCondition(condition: Expression, info: ConstructProperties): Unit = {
    condition match {
      case GreaterThan(left, right: Literal) =>
        info.addRangeInfo(left.dataType, left.sql, right.value, "GreaterThan")

      case GreaterThan(left: Literal, right) =>
        info.addRangeInfo(right.dataType, right.sql, left.value, "LessThan")

      case GreaterThanOrEqual(left, right: Literal) =>
        info.addRangeInfo(left.dataType, left.sql, right.value, "GreaterThanOrEqual")

      case GreaterThanOrEqual(left: Literal, right) =>
        info.addRangeInfo(right.dataType, right.sql, left.value, "LessThanOrEqual")

      case LessThan(left, right: Literal) =>
        info.addRangeInfo(left.dataType, left.sql, right.value, "LessThan")

      case LessThan(left: Literal, right) =>
        info.addRangeInfo(right.dataType, right.sql, left.value, "GreaterThan")

      case LessThanOrEqual(left, right: Literal) =>
        info.addRangeInfo(left.dataType, left.sql, right.value, "LessThanOrEqual")

      case LessThanOrEqual(left: Literal, right) =>
        info.addRangeInfo(right.dataType, right.sql, left.value, "GreaterThanOrEqual")

      case o => throw LakeSoulErrors.canNotCreateMaterialViewOrRewriteQueryException(
        s"Unsupport expression when parse range condition:\n ${o.sql}")
    }


  }


  /**
    * parse relation table info for material view from spark plan
    *
    * @param plan  spark plan
    * @param array result array buffer
    */
  def parseRelationTableInfo(plan: SparkPlan, array: ArrayBuffer[RelationTable]): Unit = {
    plan match {
      case BatchScanExec(_, scan) =>
        val (fileIndex, filters) = scan match {
          case fileScan: FileScan if fileScan.fileIndex.isInstanceOf[LakeSoulFileIndexV2] =>
            (fileScan.fileIndex.asInstanceOf[LakeSoulFileIndexV2], fileScan.partitionFilters)

          case mergeScan: MergeDeltaParquetScan => (mergeScan.getFileIndex, mergeScan.getPartitionFilters)

          case _ => throw LakeSoulErrors.materialViewBuildWithNonLakeSoulTableException()
        }

        val tableName = fileIndex.tableName
        val snapshot = fileIndex.snapshotManagement.snapshot
        val partitionInfo = PartitionFilter.partitionsForScan(snapshot, filters)
          .map(m => (m.range_id, m.read_version.toString))
//todo
//        if (snapshot.getTableInfo.is_material_view) {
//          throw LakeSoulErrors.materialViewBuildWithAnotherMaterialViewException()
//        }

        array += RelationTable(tableName, snapshot.getTableInfo.table_id, partitionInfo)

      case p: SparkPlan if p.children.nonEmpty => p.children.foreach(parseRelationTableInfo(_, array))

      case _ => throw LakeSoulErrors.materialViewBuildWithNonLakeSoulTableException()
    }
  }


}

