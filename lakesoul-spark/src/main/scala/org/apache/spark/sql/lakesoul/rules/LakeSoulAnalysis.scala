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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, Cast, CreateStruct, Expression, GetStructField, NamedExpression, UpCast}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.SchemaUtils
import org.apache.spark.sql.lakesoul.utils.AnalysisHelper
import org.apache.spark.sql.lakesoul.{LakeSoulTableRel, LakeSoulTableRelationV2}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

/**
  * Analysis rules for LakeSoul. Currently, these rules enable schema enforcement / evolution with
  * INSERT INTO.
  */
case class LakeSoulAnalysis(session: SparkSession, sqlConf: SQLConf)
  extends Rule[LogicalPlan] with AnalysisHelper with Logging {

  type CastFunction = (Expression, DataType) => Expression

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsDown {
    // INSERT INTO by ordinal
    case a@AppendData(DataSourceV2Relation(d: LakeSoulTableV2, _, _, _, _), query, _, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema()) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        a.copy(query = projection)
      } else {
        a
      }

    // INSERT OVERWRITE by ordinal
    case a@OverwriteByExpression(
    DataSourceV2Relation(d: LakeSoulTableV2, _, _, _, _), _, query, _, false)
      if query.resolved && needsSchemaAdjustment(d.name(), query, d.schema()) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        a.copy(query = projection)
      } else {
        a
      }

    case d@DeleteFromTable(table, condition) if d.childrenResolved =>
      val newTarget = table.transformUp { case LakeSoulRelationV2(v2r) => v2r }
      val indices = newTarget.collect {
        case LakeSoulTableRelationV2(tbl) => tbl
      }
      if (indices.isEmpty) {
        // Not a LakeSoul table at all, do not transform
        d
      } else if (indices.size == 1 && !indices.head.snapshotManagement.snapshot.isFirstCommit) {
        // It is a well-defined LakeSoul table with a schema
        LakeSoulDelete(newTarget, condition)
      } else {
        // Not a well-defined LakeSoul table
        throw LakeSoulErrors.notALakeSoulSourceException("DELETE", Some(d))
      }

    case u@UpdateTable(table, assignments, condition) if u.childrenResolved =>
      val (cols, expressions) = assignments.map(a =>
        a.key.asInstanceOf[NamedExpression] -> a.value).unzip
      val newTarget = table.transformUp { case LakeSoulRelationV2(v2r) => v2r }
      val indices = newTarget.collect {
        case LakeSoulTableRelationV2(tbl) => tbl
      }
      if (indices.isEmpty) {
        // Not a LakeSoul table at all, do not transform
        u
      } else if (indices.size == 1 && !indices.head.snapshotManagement.snapshot.isFirstCommit) {
        // It is a well-defined LakeSoul table with a schema
        LakeSoulUpdate(newTarget, cols, expressions, condition)
      } else {
        // Not a well-defined LakeSoul table
        throw LakeSoulErrors.notALakeSoulSourceException("UPDATE", Some(u))
      }
  }

  /**
    * Performs the schema adjustment by adding UpCasts (which are safe) and Aliases so that we
    * can check if the by-ordinal schema of the insert query matches our LakeSoul table.
    */
  private def normalizeQueryColumns(query: LogicalPlan, target: LakeSoulTableV2): LogicalPlan = {
    val targetAttrs = target.schema()
    // always add a Cast. it will be removed in the optimizer if it is unnecessary.
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        val expr = (attr.dataType, targetAttr.dataType) match {
          case (s, t) if s == t =>
            attr
          case (s: StructType, t: StructType) if s != t =>
            addCastsToStructs(target.name(), attr, s, t)
          case _ =>
            getCastFunction(attr, targetAttr.dataType)
        }
        Alias(expr, targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
      } else {
        attr
      }
    }
    Project(project, query)
  }

  /**
    * With LakeSoul, we ACCEPT_ANY_SCHEMA, meaning that Spark doesn't automatically adjust the schema
    * of INSERT INTO. This allows us to perform better schema enforcement/evolution. Since Spark
    * skips this step, we see if we need to perform any schema adjustment here.
    */
  private def needsSchemaAdjustment(
                                     tableName: String,
                                     query: LogicalPlan,
                                     schema: StructType): Boolean = {
    val output = query.output
    if (output.length < schema.length) {
      throw LakeSoulErrors.notEnoughColumnsInInsert(tableName, output.length, schema.length)
    }
    // Now we should try our best to match everything that already exists, and leave the rest
    // for schema evolution to WriteIntoTable
    val existingSchemaOutput = output.take(schema.length)
    existingSchemaOutput.map(_.name) != schema.map(_.name) ||
      !SchemaUtils.isReadCompatible(schema.asNullable, existingSchemaOutput.toStructType)
  }

  // Get cast operation for the level of strictness in the schema a user asked for
  private def getCastFunction: CastFunction = {
    val timeZone = conf.sessionLocalTimeZone
    conf.storeAssignmentPolicy match {
      case SQLConf.StoreAssignmentPolicy.LEGACY => Cast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.ANSI => AnsiCast(_, _, Option(timeZone))
      case SQLConf.StoreAssignmentPolicy.STRICT => UpCast(_, _)
    }
  }

  /**
    * Recursively casts structs in case it contains null types.
    * TODO: Support other complex types like MapType and ArrayType
    */
  private def addCastsToStructs(
                                 tableName: String,
                                 parent: NamedExpression,
                                 source: StructType,
                                 target: StructType): NamedExpression = {
    if (source.length < target.length) {
      throw LakeSoulErrors.notEnoughColumnsInInsert(
        tableName, source.length, target.length, Some(parent.qualifiedName))
    }
    val fields = source.zipWithIndex.map {
      case (StructField(name, nested: StructType, _, metadata), i) if i < target.length =>
        target(i).dataType match {
          case t: StructType =>
            val subField = Alias(GetStructField(parent, i, Option(name)), name)(
              explicitMetadata = Option(metadata))
            addCastsToStructs(tableName, subField, nested, t)
          case o =>
            val field = parent.qualifiedName + "." + name
            val targetName = parent.qualifiedName + "." + target(i).name
            throw LakeSoulErrors.cannotInsertIntoColumn(tableName, field, targetName, o.simpleString)
        }
      case (other, i) if i < target.length =>
        val targetAttr = target(i)
        Alias(
          getCastFunction(GetStructField(parent, i, Option(other.name)), targetAttr.dataType),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))

      case (other, i) =>
        // This is a new column, so leave to schema evolution as is. Do not lose it's name so
        // wrap with an alias
        Alias(
          GetStructField(parent, i, Option(other.name)),
          other.name)(explicitMetadata = Option(other.metadata))
    }
    Alias(CreateStruct(fields), parent.name)(
      parent.exprId, parent.qualifier, Option(parent.metadata))
  }
}

/** Matchers for dealing with a LakeSoul table. */
object LakeSoulRelation {
  def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
    case dsv2@DataSourceV2Relation(d: LakeSoulTableV2, _, _, _, _) => Some(fromV2Relation(d, dsv2))
    case lr@LakeSoulTableRel(_) => Some(lr)
    case _ => None
  }

  def fromV2Relation(d: LakeSoulTableV2, v2Relation: DataSourceV2Relation): LogicalRelation = {
    val relation = d.toBaseRelation
    LogicalRelation(relation, v2Relation.output, d.catalogTable, isStreaming = false)
  }
}

object LakeSoulRelationV2 {
  def unapply(plan: LogicalPlan): Option[DataSourceV2Relation] = plan match {
    case dsv2@DataSourceV2Relation(t: LakeSoulTableV2, _, _, _, _) => Some(dsv2)
    case _ => None
  }

}

