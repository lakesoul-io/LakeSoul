// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{CastSupport, Resolver}
import org.apache.spark.sql.catalyst.expressions.{Alias, CreateNamedStruct, Expression, GetStructField, Literal, NamedExpression}
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.types._

/**
  * Trait with helper functions to generate expressions to update target columns, even if they are
  * nested fields.
  */
trait UpdateExpressionsSupport extends CastSupport with SQLConfHelper {

  /**
    * Specifies an operation that updates a target column with the given expression.
    * The target column may or may not be a nested field and it is specified as a full quoted name
    * or as a sequence of split into parts.
    */
  case class UpdateOperation(targetColNameParts: Seq[String], updateExpr: Expression)

  protected def castIfNeeded(child: Expression, dataType: DataType): Expression = {
    child match {
      // Need to deal with NullType here, as some types cannot be casted from NullType, e.g.,
      // StructType.
      case Literal(nul, NullType) => Literal(nul, dataType)
      case otherExpr => if (child.dataType != dataType) cast(child, dataType) else child
    }
  }

  /**
    * Given a list of target-column expressions and a set of update operations, generate a list
    * of update expressions, which are aligned with given target-column expressions.
    *
    * For update operations to nested struct fields, this method recursively walks down schema tree
    * and apply the update expressions along the way.
    * For example, assume table `target` has two attributes a and z, where a is of struct type
    * with 3 fields: b, c and d, and z is of integer type.
    *
    * Given an update command:
    *
    *  - UPDATE target SET a.b = 1, a.c = 2, z = 3
    *
    * this method works as follows:
    *
    * generateUpdateExpressions(targetCols=[a,z], updateOps=[(a.b, 1), (a.c, 2), (z, 3)])
    * generateUpdateExpressions(targetCols=[b,c,d], updateOps=[(b, 1),(c, 2)], pathPrefix=["a"])
    * end-of-recursion
    * -> returns (1, 2, d)
    * -> return ((1, 2, d), 3)
    *
    * @param targetCols a list of expressions to read named columns; these named columns can be
    *                   either the top-level attributes of a table, or the nested fields of a
    *                   StructType column.
    * @param updateOps  a set of update operations.
    * @param pathPrefix the path from root to the current (nested) column. Only used for printing out
    *                   full column path in error messages.
    */
  protected def generateUpdateExpressions(
                                           targetCols: Seq[NamedExpression],
                                           updateOps: Seq[UpdateOperation],
                                           resolver: Resolver,
                                           pathPrefix: Seq[String] = Nil): Seq[Expression] = {
    // Check that the head of nameParts in each update operation can match a target col. This avoids
    // silently ignoring invalid column names specified in update operations.
    updateOps.foreach { u =>
      if (!targetCols.exists(f => resolver(f.name, u.targetColNameParts.head))) {
        throw LakeSoulErrors.updateSetColumnNotFoundException(
          (pathPrefix :+ u.targetColNameParts.head).mkString("."),
          targetCols.map(col => (pathPrefix :+ col.name).mkString(".")))
      }
    }

    // Transform each targetCol to a possibly updated expression
    targetCols.map { targetCol =>
      // The prefix of a update path matches the current targetCol path.
      val prefixMatchedOps =
        updateOps.filter(u => resolver(u.targetColNameParts.head, targetCol.name))
      // No prefix matches this target column, return its original expression.
      if (prefixMatchedOps.isEmpty) {
        targetCol
      } else {
        // The update operation whose path exactly matches the current targetCol path.
        val fullyMatchedOp = prefixMatchedOps.find(_.targetColNameParts.size == 1)
        if (fullyMatchedOp.isDefined) {
          // If a full match is found, then it should be the ONLY prefix match. Any other match
          // would be a conflict, whether it is a full match or prefix-only. For example,
          // when users are updating a nested column a.b, they can't simultaneously update a
          // descendant of a.b, such as a.b.c.
          if (prefixMatchedOps.size > 1) {
            throw LakeSoulErrors.updateSetConflictException(
              prefixMatchedOps.map(op => (pathPrefix ++ op.targetColNameParts).mkString(".")))
          }
          // For an exact match, return the updateExpr from the update operation.
          castIfNeeded(fullyMatchedOp.get.updateExpr, targetCol.dataType)
        } else {
          // So there are prefix-matched update operations, but none of them is a full match. Then
          // that means targetCol is a complex data type, so we recursively pass along the update
          // operations to its children.
          targetCol.dataType match {
            case StructType(fields) =>
              val fieldExpr = targetCol
              val childExprs = fields.zipWithIndex.map { case (field, ordinal) =>
                Alias(GetStructField(fieldExpr, ordinal, Some(field.name)), field.name)()
              }
              // Recursively apply update operations to the children
              val updatedChildExprs = generateUpdateExpressions(
                childExprs,
                prefixMatchedOps.map(u => u.copy(targetColNameParts = u.targetColNameParts.tail)),
                resolver,
                pathPrefix :+ targetCol.name)
              // Reconstruct the expression for targetCol using its possibly updated children
              val namedStructExprs = fields
                .zip(updatedChildExprs)
                .flatMap { case (field, expr) => Seq(Literal(field.name), expr) }
              CreateNamedStruct(namedStructExprs)

            case otherType =>
              throw LakeSoulErrors.updateNonStructTypeFieldNotSupportedException(
                (pathPrefix :+ targetCol.name).mkString("."), otherType)
          }
        }
      }
    }
  }

  protected def generateUpdateExpressions(targetCols: Seq[NamedExpression],
                                          nameParts: Seq[Seq[String]],
                                          updateExprs: Seq[Expression],
                                          resolver: Resolver): Seq[Expression] = {
    assert(nameParts.size == updateExprs.size)
    val updateOps = nameParts.zip(updateExprs).map {
      case (nameParts, expr) => UpdateOperation(nameParts, expr)
    }
    generateUpdateExpressions(targetCols, updateOps, resolver)
  }
}
