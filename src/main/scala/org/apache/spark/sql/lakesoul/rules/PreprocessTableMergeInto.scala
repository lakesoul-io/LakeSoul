package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, InsertAction, LakeSoulUpsert, LogicalPlan, MergeAction, MergeIntoTable, UpdateAction}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulTableRelationV2
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors

case class PreprocessTableMergeInto(sqlConf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m@MergeIntoTable(targetTable, sourceTable, mergeCondition, matchedActions, notMatchedActions)
      if m.resolved =>

      EliminateSubqueryAliases(targetTable) match {
        case LakeSoulTableRelationV2(tbl: LakeSoulTableV2) =>
          if (lakeSoulTableHasHashPartition(tbl)
            && isMergeConditionOnPrimaryKey(mergeCondition, tbl)
            && matchedActionIsOneUpdateOnly(matchedActions)
            && notMatchedActionIsOneInsertOnly(notMatchedActions))
          {
            logInfo(s"Merge into ${tbl.name()} is optimized to Upsert")
            LakeSoulUpsert(targetTable, sourceTable, "")
          } else {
            throw LakeSoulErrors.operationNotSupportedException("Merge into with (not)matched conditions")
          }
      }
  }

  private def isMergeConditionOnPrimaryKey(mergeCondition: Expression, tbl: LakeSoulTableV2): Boolean = {
    val hashColumnNameHit = scala.collection.mutable.Map(
      tbl.snapshotManagement.snapshot.getTableInfo
        .hash_partition_schema.fieldNames.map(k => k -> false): _*)
    var notQualifiedCondition = false
    mergeCondition foreachUp {
      case EqualTo(left: AttributeReference, right: AttributeReference)
        if left.name == right.name =>
        if (hashColumnNameHit.contains(left.name)) hashColumnNameHit(left.name) = true
        else notQualifiedCondition = true
      case And(_, _) | AttributeReference(_, _, _, _) =>
      case _ => notQualifiedCondition = true
    }
    !notQualifiedCondition && hashColumnNameHit.forall(_._2)
  }

  private def lakeSoulTableHasHashPartition(table: LakeSoulTableV2): Boolean = {
    table.snapshotManagement.snapshot.getTableInfo.hash_column.nonEmpty
  }

  private def assignmentsIsAttributeOnly(assignments: Seq[Assignment]): Boolean = {
    assignments.forall( a => a.key.isInstanceOf[AttributeReference]
      && a.value.isInstanceOf[AttributeReference])
  }

  private def matchedActionIsOneUpdateOnly(matchedAction: Seq[MergeAction]): Boolean = {
    matchedAction match {
      case Seq(UpdateAction(condition, assignments))
        if condition.isEmpty && assignmentsIsAttributeOnly(assignments) =>
        true
      case _ => false
    }
  }

  private def notMatchedActionIsOneInsertOnly(notMatchedAction: Seq[MergeAction]): Boolean = {
    notMatchedAction match {
      case Seq(InsertAction(condition, assignments))
        if condition.isEmpty && assignmentsIsAttributeOnly(assignments) =>
        true
      case _ => false
    }
  }

}
