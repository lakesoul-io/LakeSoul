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

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.{LakeSoulCatalog, LakeSoulTableV2}

case class LakeSoulPostHocAnalysis(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    /**
      * Hash columns in LakeSoulTableRel are primary keys, they have no duplicate data and shouldn't be null,
      * so [[Intersect]]/[[Except]] operator can be replaced with a left-semi/left-anti [[Join]] operator.
      * {{{
      *   SELECT a1, a2 FROM Tab1 INTERSECT/EXCEPT (ALL) SELECT b1, b2 FROM Tab2
      *   ==>  SELECT a1, a2 FROM Tab1 LEFT SEMI/ANTI JOIN Tab2 ON a1=b1 AND a2=b2
      * }}}
      *
      * Note:
      * This rule is only applicable to INTERSECT/EXCEPT LakeSoulTableRel hash columns.
      */
    case Intersect(left, right, _) =>
      assert(left.output.size == right.output.size)
      val transLeft = EliminateSubqueryAliases(left)
      val transRight = EliminateSubqueryAliases(right)
      //make sure the output Attributes are hash columns
      val leftInfo = findLakeSoulRelation(transLeft, transLeft.references.map(_.name).toSet)
      val rightInfo = findLakeSoulRelation(transRight, transRight.references.map(_.name).toSet)
      val canOptimize = leftInfo._1 && rightInfo._1 && (leftInfo._2 == rightInfo._2)

      if (canOptimize) {
        val joinCond = left.output.zip(right.output).map { case (l, r) => EqualTo(l, r) }
        Join(left, right, LeftSemi, joinCond.reduceLeftOption(And), JoinHint.NONE)
      } else {
        plan
      }

    case Except(left, right, _) =>
      assert(left.output.size == right.output.size)
      val transLeft = EliminateSubqueryAliases(left)
      val transRight = EliminateSubqueryAliases(right)
      //make sure the output Attributes are hash columns
      val leftInfo = findLakeSoulRelation(transLeft, transLeft.references.map(_.name).toSet)
      val rightInfo = findLakeSoulRelation(transRight, transRight.references.map(_.name).toSet)
      val canOptimize = leftInfo._1 && rightInfo._1 && (leftInfo._2 == rightInfo._2)

      if (canOptimize) {
        val joinCond = left.output.zip(right.output).map { case (l, r) => EqualTo(l, r) }
        Join(left, right, LeftAnti, joinCond.reduceLeftOption(And), JoinHint.NONE)
      } else {
        plan
      }
  }

  def findLakeSoulRelation(plan: LogicalPlan, outCols: Set[String]): (Boolean, Int) = {
    plan match {
      case DataSourceV2Relation(LakeSoulTableV2(_, path, _, _, _, _), _, _, _, _) =>
        val tableInfo = SnapshotManagement(path, LakeSoulCatalog.showCurrentNamespace().mkString(".")).getTableInfoOnly
        val hashCols = tableInfo.hash_partition_columns.toSet
        if (hashCols.equals(outCols) && tableInfo.bucket_num != -1) {
          (true, tableInfo.bucket_num)
        } else {
          (false, -1)
        }

      case lp: LogicalPlan =>
        if (lp.children.size == 1) {
          findLakeSoulRelation(lp.children.head, outCols)
        } else {
          (false, -1)
        }
    }
  }

}


