// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, NonSQLExpression, UnaryExpression}
import org.apache.spark.sql.lakesoul.schema.Invariants.{ArbitraryExpression, NotNull}
import org.apache.spark.sql.types.{DataType, NullType}

/** An expression that validates a specific invariant on a column, before writing into lakesoul table. */
case class CheckInvariant(child: Expression,
                          invariant: Invariant) extends UnaryExpression with NonSQLExpression {

  override def dataType: DataType = NullType

  override def foldable: Boolean = false

  override def nullable: Boolean = true

  override def flatArguments: Iterator[Any] = Iterator(child)

  private def assertRule(input: InternalRow): Unit = invariant.rule match {
    case NotNull if child.eval(input) == null =>
      throw InvariantViolationException(invariant, "")
    case ArbitraryExpression(expr) =>
      val resolvedExpr = expr.transform {
        case _: UnresolvedAttribute => child
      }
      val result = resolvedExpr.eval(input)
      if (result == null || result == false) {
        throw InvariantViolationException(
          invariant, s"Value ${child.eval(input)} violates requirement.")
      }
  }

  override def eval(input: InternalRow): Any = {
    assertRule(input)
    null
  }

  private def generateNotNullCode(ctx: CodegenContext): Block = {
    val childGen = child.genCode(ctx)
    val invariantField = ctx.addReferenceObj("errMsg", invariant)
    code"""${childGen.code}
          |
       |if (${childGen.isNull}) {
          |  throw org.apache.spark.sql.lakesoul.schema.InvariantViolationException.apply(
          |    $invariantField, "");
          |}
     """.stripMargin
  }

  private def generateExpressionValidationCode(expr: Expression, ctx: CodegenContext): Block = {
    val resolvedExpr = expr.transform {
      case _: UnresolvedAttribute => child
    }
    val elementValue = child.genCode(ctx)
    val childGen = resolvedExpr.genCode(ctx)
    val invariantField = ctx.addReferenceObj("errMsg", invariant)
    val eValue = ctx.freshName("elementResult")
    code"""${elementValue.code}
          |${childGen.code}
          |
       |if (${childGen.isNull}|| ${childGen.value} == false) {
          |  Object $eValue = "null";
          |  if (!${elementValue.isNull}) {
          |    $eValue = (Object) ${elementValue.value};
          |  }
          |  throw org.apache.spark.sql.lakesoul.schema.InvariantViolationException.apply(
          |     $invariantField, "Value " + $eValue + " violates requirement.");
          |}
     """.stripMargin
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val code = invariant.rule match {
      case NotNull => generateNotNullCode(ctx)
      case ArbitraryExpression(expr) => generateExpressionValidationCode(expr, ctx)
    }
    ev.copy(code = code, isNull = TrueLiteral, value = JavaCode.literal("null", NullType))
  }

  override protected def withNewChildInternal(newChild: Expression): Expression = {
    copy(child = newChild)
  }
}
