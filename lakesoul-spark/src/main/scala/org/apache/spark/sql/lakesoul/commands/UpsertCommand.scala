// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER, LAKESOUL_RANGE_PARTITION_SPLITTER}
import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.lakesoul._
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.SparkUtil
//import org.apache.spark.sql.lakesoul.actions.AddFile
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.schema.ImplicitMetadataOperation
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.AnalysisHelper
import org.apache.spark.sql.types.StructType

/**
  * Performs a Upsert of a source query/table into a LakeSoul table.
  *
  * @param source                   Source data to merge from
  * @param target                   Target table to merge into
  * @param targetSnapshotManagement snapshotManagement of the target table
  * @param conditionString          Condition for a source row to match with a target row
  * @param migratedSchema           The final schema of the target - may be changed by schema evolution.
  */
case class UpsertCommand(source: LogicalPlan,
                         target: LogicalPlan,
                         targetSnapshotManagement: SnapshotManagement,
                         conditionString: String,
                         migratedSchema: Option[StructType]) extends RunnableCommand
  with LeafRunnableCommand with PredicateHelper with AnalysisHelper with ImplicitMetadataOperation {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target, source)

  private val tableInfo = targetSnapshotManagement.snapshot.getTableInfo

  override val canMergeSchema: Boolean = conf.getConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false
  override val rangePartitions: String = tableInfo.range_column
  override val hashPartitions: String = tableInfo.hash_column
  override val hashBucketNum: Int = tableInfo.bucket_num
  override val shortTableName: Option[String] = None


  final override def run(spark: SparkSession): Seq[Row] = {
    val condition = conditionString match {
      case "" => Literal(true)
      case _ => expr(conditionString).expr
    }
    targetSnapshotManagement.withNewTransaction { tc =>
      if (target.schema.size != tableInfo.schema.size) {
        throw LakeSoulErrors.schemaChangedSinceAnalysis(
          atAnalysis = target.schema, latestSchema = tableInfo.schema)
      }

      if (tableInfo.hash_column.isEmpty) {
        throw LakeSoulErrors.hashColumnsIsNullException()
      }

      val canUseDeltaFile = spark.conf.get(LakeSoulSQLConf.USE_DELTA_FILE)

      val sourceCols = source.output.map(_.name.stripPrefix("`").stripSuffix("`"))

      //source schema should have all the partition cols
      if (!tableInfo.partition_cols.forall(sourceCols.contains)) {
        throw LakeSoulErrors
          .partitionColumnNotFoundException(
            tableInfo.partition_cols.mkString(LAKESOUL_RANGE_PARTITION_SPLITTER),
            sourceCols.mkString(","))
      }

      if (canMergeSchema) {
        updateMetadata(
          spark,
          tc,
          migratedSchema.getOrElse(target.schema),
          tc.tableInfo.configuration,
          isOverwriteMode = false)
      } else {
        val externalColumns = sourceCols.filterNot(tableInfo.schema.fieldNames.contains)
        if (externalColumns.nonEmpty) {
          throw LakeSoulErrors.columnsNotFoundException(externalColumns)
        }
      }


      /** If delta file can be used, just write new data and delete nothing.
        * Else a merge data should be built and overwrite all files. */
      if (canUseDeltaFile) {
        tc.setCommitType("merge")

        val newFiles = tc.writeFiles(Dataset.ofRows(spark, source))
        tc.commit(newFiles, Seq.empty[DataFileInfo])
      } else {
        tc.setCommitType("update")
        val targetOnlyPredicates = splitConjunctivePredicates(condition)
          .filter(f =>
            f.references.nonEmpty
              && f.references.forall(r => tableInfo.range_partition_columns.contains(r.name)))

        //condition should be declared for partitioned table by default
        if (tableInfo.range_column.nonEmpty
          && targetOnlyPredicates.isEmpty
          && !conf.getConf(LakeSoulSQLConf.ALLOW_FULL_TABLE_UPSERT)) {
          throw LakeSoulErrors.upsertConditionNotFoundException()
        }

        val dataSkippedFiles = tc.filterFiles(targetOnlyPredicates)

        val targetExistCols = dataSkippedFiles.flatMap(_.file_exist_cols.split(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER)).distinct
        val needColumns = tableInfo.schema.fieldNames
        val repeatCols = sourceCols.intersect(targetExistCols)
        val allCols = sourceCols.union(targetExistCols).distinct


        val columnFilter = new Column(targetOnlyPredicates.reduceLeftOption(And).getOrElse(Literal(true)))
        val sourceDF = Dataset.ofRows(spark, source).filter(columnFilter)

        val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(tc, dataSkippedFiles, needColumns))

        var resultDF = targetDF.join(sourceDF, tableInfo.partition_cols, "full")

        if (repeatCols.nonEmpty) {
          resultDF = resultDF.select(allCols.map(column => {
            if (repeatCols.contains(column) && !tableInfo.partition_cols.contains(column)) {
              coalesce(sourceDF(column), targetDF(column)).as(column)
            } else {
              col(column)
            }
          }): _*)
        }

        val newFiles = tc.writeFiles(resultDF)
        tc.commit(newFiles, dataSkippedFiles, targetSnapshotManagement.snapshot.getPartitionInfoArray)
      }
    }
    spark.sharedState.cacheManager.recacheByPlan(spark, target)
    Seq.empty
  }


  /**
    * Build a new logical plan using the given `files` that has the same output columns (exprIds)
    * as the `target` logical plan, so that existing update/insert expressions can be applied
    * on this new plan.
    */
  private def buildTargetPlanWithFiles(tc: TransactionCommit,
                                       files: Seq[DataFileInfo],
                                       selectCols: Seq[String]): LogicalPlan = {
    val plan = SparkUtil
      .createDataFrame(files, selectCols, tc.snapshotManagement)
      .queryExecution.analyzed

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = getTargetOutputCols.find(_.name == newAttrib.name)
          .getOrElse {
            throw new AnalysisException(
              s"Could not find ${newAttrib.name} among the existing target output " +
                s"$getTargetOutputCols")
          }.asInstanceOf[AttributeReference]
        Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
    }
    Project(aliases, plan)
  }

  private def getTargetOutputCols: Seq[NamedExpression] = {
    tableInfo.schema.map { col =>
      target.output.find(attr => conf.resolver(attr.name, col.name)).getOrElse {
        Alias(Literal(null, col.dataType), col.name)()
      }
    }
  }

}

