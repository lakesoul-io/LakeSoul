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

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.MetaUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.rules.LakeSoulRelation
import org.apache.spark.sql.lakesoul.sources.{LakeSoulBaseRelation, LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.DataFileInfo
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.Utils


object LakeSoulUtils extends PredicateHelper {

  val MERGE_OP_COL = "_lakesoul_merge_col_name_"
  val MERGE_OP = "_lakesoul_merge_op_"

  lazy val USE_MATERIAL_REWRITE = "_lakesoul_use_material_rewrite_"


  def executeWithoutQueryRewrite[T](sparkSession: SparkSession)(f: => T): Unit = {
    sparkSession.conf.set(USE_MATERIAL_REWRITE, "false")
    f
    sparkSession.conf.set(USE_MATERIAL_REWRITE, "true")
  }

  def enableAsyncIO(tablePath: String, conf: SQLConf): Boolean = {
    val validFormat = tablePath.startsWith("s3") || tablePath.startsWith("oss")
    validFormat && conf.getConf(LakeSoulSQLConf.ASYNC_IO_ENABLE)
  }

  def getClass(className: String): Class[_] = {
    Class.forName(className, true, Utils.getContextOrSparkClassLoader)
  }

  /** return async class */
  def getAsyncClass(className: String): (Boolean, Class[_]) = {
    try {
      val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
      (true, cls)
    } catch {
      case e: ClassNotFoundException => (false, null)
      case e: Exception => throw e
    }
  }

  /** Check whether this table is a LakeSoulTableRel based on information from the Catalog. */
  def isLakeSoulTable(table: CatalogTable): Boolean = LakeSoulSourceUtils.isLakeSoulTable(table.provider)

  /**
    * Check whether the provided table name is a lakesoul table based on information from the Catalog.
    */
  def isLakeSoulTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTemporaryTable(tableName)
    val tableExists =
      (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
        catalog.tableExists(tableName)
    tableIsNotTemporaryTable && tableExists && isLakeSoulTable(catalog.getTableMetadata(tableName))
  }

  /** Check if the provided path is the root or the children of a lakesoul table. */
  def isLakeSoulTable(spark: SparkSession, path: Path): Boolean = {
    findTableRootPath(spark, path).isDefined
  }

  def isLakeSoulTable(tablePath: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    isLakeSoulTable(sparkSession, new Path(MetaUtils.modifyTableString(tablePath)))
  }

  def findTableRootPath(spark: SparkSession, path: Path): Option[Path] = {
    var current_path = path
    while (current_path != null) {
      if (LakeSoulSourceUtils.isLakeSoulTableExists(current_path.toString)) {
        return Option(current_path)
      }
      current_path = current_path.getParent
    }
    None
  }

  /**
    * Partition the given condition into two sequence of conjunctive predicates:
    * - predicates that can be evaluated using metadata only.
    * - other predicates.
    */
  def splitMetadataAndDataPredicates(condition: Expression,
                                     partitionColumns: Seq[String],
                                     spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition(
      isPredicateMetadataOnly(_, partitionColumns, spark))
  }

  /**
    * Check if condition can be evaluated using only metadata. In LakeSoulTableRel, this means the condition
    * only references partition columns and involves no subquery.
    */
  def isPredicateMetadataOnly(condition: Expression,
                              partitionColumns: Seq[String],
                              spark: SparkSession): Boolean = {
    isPredicatePartitionColumnsOnly(condition, partitionColumns, spark) &&
      !containsSubquery(condition)
  }

  def isPredicatePartitionColumnsOnly(condition: Expression,
                                      partitionColumns: Seq[String],
                                      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }


  def containsSubquery(condition: Expression): Boolean = {
    SubqueryExpression.hasSubquery(condition)
  }


  /**
    * Replace the file index in a logical plan and return the updated plan.
    * It's a common pattern that, in LakeSoulTableRel commands, we use data skipping to determine a subset of
    * files that can be affected by the command, so we replace the whole-table file index in the
    * original logical plan with a new index of potentially affected files, while everything else in
    * the original plan, e.g., resolved references, remain unchanged.
    *
    * @param target the logical plan in which we replace the file index
    */

  def replaceFileIndex(target: LogicalPlan,
                       files: Seq[DataFileInfo]): LogicalPlan = {
    target transform {
      case l@LogicalRelation(egbr: LakeSoulBaseRelation, _, _, _) =>
        l.copy(relation = egbr.copy(files = files)(egbr.sparkSession))
    }
  }

  def replaceFileIndexV2(target: LogicalPlan,
                         files: Seq[DataFileInfo]): LogicalPlan = {
    EliminateSubqueryAliases(target) match {
      case sr@DataSourceV2Relation(tbl: LakeSoulTableV2, _, _, _, _) =>
        sr.copy(table = tbl.copy(userDefinedFileIndex = Option(BatchDataSoulFileIndexV2(tbl.spark, tbl.snapshotManagement, files))))

      case _ => throw LakeSoulErrors.lakeSoulRelationIllegalException()
    }
  }


  /** Whether a path should be hidden for lakesoul-related file operations, such as cleanup. */
  def isHiddenDirectory(partitionColumnNames: Seq[String], pathName: String): Boolean = {
    // Names of the form partitionCol=[value] are partition directories, and should be
    // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    // indexes and these must be GCed when the data they are tied to is GCed.
    (pathName.startsWith(".") || pathName.startsWith("_")) &&
      !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
  }


}


/**
  * Extractor Object for pulling out the table scan of a LakeSoulTableRel. It could be a full scan
  * or a partial scan.
  */
object LakeSoulTableRel {
  def unapply(a: LogicalRelation): Option[LakeSoulBaseRelation] = a match {
    case LogicalRelation(epbr: LakeSoulBaseRelation, _, _, _) =>
      Some(epbr)
    case _ =>
      None
  }
}


/**
  * Extractor Object for pulling out the full table scan of a LakeSoul table.
  */
object LakeSoulFullTable {
  def unapply(a: LogicalPlan): Option[LakeSoulBaseRelation] = a match {
    case PhysicalOperation(_, filters, lr@LakeSoulTableRel(epbr: LakeSoulBaseRelation)) =>
      if (epbr.snapshotManagement.snapshot.isFirstCommit) return None
      if (filters.isEmpty) {
        Some(epbr)
      } else {
        throw new AnalysisException(
          s"Expect a full scan of LakeSoul sources, but found a partial scan. " +
            s"path:${epbr.snapshotManagement.table_name}")
      }
    // Convert V2 relations to V1 and perform the check
    case LakeSoulRelation(lr) => unapply(lr)
    case _ => None
  }
}


object LakeSoulTableRelationV2 {
  def unapply(plan: LogicalPlan): Option[LakeSoulTableV2] = plan match {
    case DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) => Some(table)
    case DataSourceV2ScanRelation(DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _), _, _) => Some(table)
    case _ => None
  }
}

object LakeSoulTableV2ScanRelation {
  def unapply(plan: LogicalPlan): Option[DataSourceV2ScanRelation] = plan match {
    case dsv2@DataSourceV2Relation(t: LakeSoulTableV2, _, _, _, _) => Some( createScanRelation(t, dsv2))
    case _ => None
  }

  def createScanRelation(table: LakeSoulTableV2, v2Relation: DataSourceV2Relation): DataSourceV2ScanRelation = {
    DataSourceV2ScanRelation(
      v2Relation,
      table.newScanBuilder(v2Relation.options).build(),
      v2Relation.output)
  }
}

object LakeSoulTableProperties {

  val lakeSoulCDCChangePropKey = "lakesoul_cdc_change_column"

  val extraTblProps = Set(lakeSoulCDCChangePropKey)

  def isLakeSoulTableProperty(name: String): Boolean = {
    extraTblProps.contains(name)
  }
}

class MergeOpLong extends MergeOperator[Long] {
  override def mergeData(input: Seq[Long]): Long = {
    input.sum
  }
}