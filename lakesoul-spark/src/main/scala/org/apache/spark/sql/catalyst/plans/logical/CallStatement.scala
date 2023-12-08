// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Expression

case class CallStatement(name: Seq[String], args: Seq[CallArgument]) extends LeafParsedStatement
sealed trait CallArgument {
  def expr: Expression
}
/**
 * An argument in a CALL statement identified by name.
 */
case class NamedArgument(name: String, expr: Expression) extends CallArgument

/**
 * An argument in a CALL statement identified by position.
 */
case class PositionalArgument(expr: Expression) extends CallArgument