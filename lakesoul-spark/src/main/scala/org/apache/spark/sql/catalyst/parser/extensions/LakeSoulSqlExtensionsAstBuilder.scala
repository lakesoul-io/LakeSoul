// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions

import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}
import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.LakeSoulParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.{CallArgument, CallStatement, LogicalPlan,NamedArgument,PositionalArgument}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}

import scala.collection.JavaConverters._


class LakeSoulSqlExtensionsAstBuilder(delegate: ParserInterface) extends LakeSoulSqlExtensionsBasicVisitor[AnyRef]{
  private def toSeq[T](list: java.util.List[T]): Seq[T] = toBuffer(list).toSeq
  private def toBuffer[T](list: java.util.List[T]): scala.collection.mutable.Buffer[T] = list.asScala

  override def visitSingleStatement(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.SingleStatementContext): LogicalPlan = {
    visit(ctx.statement).asInstanceOf[LogicalPlan]

  }
  override def visitCall(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.CallContext): CallStatement = withOrigin(ctx) {
    val name = toSeq(ctx.multipartIdentifier.parts).map(_.getText)
    val args = toSeq(ctx.callArgument).map(typedVisit[CallArgument])
    CallStatement(name, args)
  }
  override def visitPositionalArgument(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.PositionalArgumentContext): CallArgument = withOrigin(ctx) {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }
  override def visitNamedArgument(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.NamedArgumentContext): NamedArgument = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val expr = typedVisit[Expression](ctx.expression)
    NamedArgument(name, expr)
  }

  override def visitExpression(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  override def visitMultipartIdentifier(ctx: org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.MultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    toSeq(ctx.parts).map(_.getText)
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    toBuffer(ctx.children).map {
      case c: ParserRuleContext => reconstructSqlString(c)
      case t: TerminalNode => t.getText
    }.mkString(" ")
  }

}


object LakeSoulParserUtils {

  private[sql] def withOrigin[T](ctx: ParserRuleContext)(f: => T): T = {
    val current = CurrentOrigin.get
    CurrentOrigin.set(position(ctx.getStart))
    try {
      f
    } finally {
      CurrentOrigin.set(current)
    }
  }

  private[sql] def position(token: Token): Origin = {
    val opt = Option(token)
    Origin(opt.map(_.getLine), opt.map(_.getCharPositionInLine))
  }

  /** Get the command which created the token. */
  private[sql] def command(ctx: ParserRuleContext): String = {
    val stream = ctx.getStart.getInputStream
    stream.getText(Interval.of(0, stream.size() - 1))
  }
}