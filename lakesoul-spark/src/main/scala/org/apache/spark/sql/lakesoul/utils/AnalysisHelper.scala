package org.apache.spark.sql.lakesoul.utils

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.{Dataset, Row, SparkSession}

trait AnalysisHelper {

  import AnalysisHelper._

  protected def tryResolveReferences(sparkSession: SparkSession)(
    expr: Expression,
    planContainingExpr: LogicalPlan): Expression = {
    val newPlan = FakeLogicalPlan(expr, planContainingExpr.children)
    sparkSession.sessionState.analyzer.execute(newPlan) match {
      case FakeLogicalPlan(resolvedExpr, _) =>
        // Return even if it did not successfully resolve
        resolvedExpr
      case _ =>
        // This is unexpected
        throw LakeSoulErrors.analysisException(
          s"Could not resolve expression $expr", plan = Option(planContainingExpr))
    }
  }

  protected def toDataset(sparkSession: SparkSession, logicalPlan: LogicalPlan): Dataset[Row] = {
    Dataset.ofRows(sparkSession, logicalPlan)
  }

  protected def improveUnsupportedOpError(f: => Unit): Unit = {
    val possibleErrorMsgs = Seq(
      "is only supported with v2 table", // full error: DELETE is only supported with v2 tables
      "is not supported temporarily", // full error: UPDATE TABLE is not supported temporarily
      "Table does not support read",
      "Table implementation does not support writes"
    ).map(_.toLowerCase())

    def isExtensionOrCatalogError(error: Exception): Boolean = {
      possibleErrorMsgs.exists(m => error.getMessage().toLowerCase().contains(m))
    }

    try {
      f
    } catch {
      case e: Exception if isExtensionOrCatalogError(e) =>
        throw LakeSoulErrors.configureSparkSessionWithExtensionAndCatalog(e)
    }
  }
}

object AnalysisHelper {

  /** LogicalPlan to help resolve the given expression */
  case class FakeLogicalPlan(expr: Expression, children: Seq[LogicalPlan])
    extends LogicalPlan {
    override def output: Seq[Attribute] = Nil
  }

}

