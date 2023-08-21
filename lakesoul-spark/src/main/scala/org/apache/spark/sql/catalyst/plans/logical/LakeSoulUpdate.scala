// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExtractValue, GetStructField, NamedExpression}

/**
  * Perform UPDATE on a table
  *
  * @param child             the logical plan representing target table
  * @param updateColumns     : the to-be-updated target columns
  * @param updateExpressions : the corresponding update expression if the condition is matched
  * @param condition         : Only rows that match the condition will be updated
  */
case class LakeSoulUpdate(child: LogicalPlan,
                          updateColumns: Seq[NamedExpression],
                          updateExpressions: Seq[Expression],
                          condition: Option[Expression])
  extends UnaryNode {

  assert(updateColumns.size == updateExpressions.size)

  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}


object LakeSoulUpdate {
  /**
    * Extracts name parts from a resolved expression referring to a nested or non-nested column
    * - For non-nested column, the resolved expression will be like `AttributeReference(...)`.
    * - For nested column, the resolved expression will be like `Alias(GetStructField(...))`.
    *
    * In the nested case, the function recursively traverses through the expression to find
    * the name parts. For example, a nested field of a.b.c would be resolved to an expression
    *
    * `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
    *
    * for which this method recursively extracts the name parts as follows:
    *
    * `Alias(c, GetStructField(c, GetStructField(b, AttributeReference(a)))`
    * ->  `GetStructField(c, GetStructField(b, AttributeReference(a)))`
    * ->  `GetStructField(b, AttributeReference(a))` ++ Seq(c)
    * ->  `AttributeReference(a)` ++ Seq(b, c)
    * ->  [a, b, c]
    */
  def getTargetColNameParts(resolvedTargetCol: Expression, errMsg: String = null): Seq[String] = {

    def fail(extraMsg: String): Nothing = {
      val msg = Option(errMsg).map(_ + " - ").getOrElse("") + extraMsg
      throw new AnalysisException(msg)
    }

    def extractRecursively(expr: Expression): Seq[String] = expr match {
      case attr: Attribute => Seq(attr.name)

      case Alias(c, _) => extractRecursively(c)

      case GetStructField(c, _, Some(name)) => extractRecursively(c) :+ name

      case _: ExtractValue =>
        fail("Updating nested fields is only supported for StructType.")

      case other =>
        fail(s"Found unsupported expression '$other' while parsing target column name parts")
    }

    extractRecursively(resolvedTargetCol)
  }
}
