// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BindReferences, Expression, GetStructField, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.Invariants.NotNull
import org.apache.spark.sql.types.{NullType, StructType}

/**
  * A physical operator that validates records, before they are written into LakeSoulTableRel. Each row
  * is left unchanged after validations.
  */
case class InvariantCheckerExec(child: SparkPlan,
                                invariants: Seq[Invariant]) extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  private def isNullNotOkay(invariant: Invariant): Boolean = invariant.rule match {
    case NotNull => true
    case _ => false
  }

  /** Build extractors to access the column an invariant is defined on. */
  private def buildExtractors(invariant: Invariant): Option[Expression] = {
    assert(invariant.column.nonEmpty)
    val topLevelColumn = invariant.column.head
    val topLevelRefOpt = output.collectFirst {
      case a: AttributeReference if SchemaUtils.COL_RESOLVER(a.name, topLevelColumn) => a
    }
    val rejectColumnNotFound = isNullNotOkay(invariant)
    if (topLevelRefOpt.isEmpty) {
      if (rejectColumnNotFound) {
        throw LakeSoulErrors.notNullInvariantException(invariant)
      }
    }

    if (invariant.column.length == 1) {
      topLevelRefOpt.map(BindReferences.bindReference[Expression](_, output))
    } else {
      topLevelRefOpt.flatMap { topLevelRef =>
        val boundTopLevel = BindReferences.bindReference[Expression](topLevelRef, output)
        try {
          val nested = invariant.column.tail.foldLeft(boundTopLevel) { case (e, fieldName) =>
            e.dataType match {
              case StructType(fields) =>
                val ordinal = fields.indexWhere(f =>
                  SchemaUtils.COL_RESOLVER(f.name, fieldName))
                if (ordinal == -1) {
                  throw new IndexOutOfBoundsException(s"Not nullable column not found in struct: " +
                    s"${fields.map(_.name).mkString("[", ",", "]")}")
                }
                GetStructField(e, ordinal, Some(fieldName))
              case _ =>
                throw new UnsupportedOperationException(
                  "Invariants on nested fields other than StructTypes are not supported.")
            }
          }
          Some(nested)
        } catch {
          case i: IndexOutOfBoundsException if rejectColumnNotFound =>
            throw InvariantViolationException(invariant, i.getMessage)
          case _: IndexOutOfBoundsException if !rejectColumnNotFound =>
            None
        }
      }
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (invariants.isEmpty) return child.execute()
    val boundRefs = invariants.map { invariant =>
      CheckInvariant(buildExtractors(invariant).getOrElse(Literal(null, NullType)), invariant)
    }

    child.execute().mapPartitionsInternal { rows =>
      val assertions = GenerateUnsafeProjection.generate(boundRefs)
      rows.map { row =>
        assertions(row)
        row
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
}
