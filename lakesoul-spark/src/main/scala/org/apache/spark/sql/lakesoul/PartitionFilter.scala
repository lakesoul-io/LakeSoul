// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{DataFileInfo, DataOperation, MetaUtils}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.lakesoul.utils.{PartitionFilterInfo, SparkUtil}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

object PartitionFilter {

  def partitionsForScan(snapshot: Snapshot, filters: Seq[Expression]): Seq[PartitionFilterInfo] = {
    val table_info = snapshot.getTableInfo

    val spark = SparkSession.active
    val partitionFilters = filters.flatMap { filter =>
      LakeSoulUtils.splitMetadataAndDataPredicates(filter, table_info.range_partition_columns, spark)._1
    }
    val allPartitions = SparkUtil.allPartitionFilterInfoDF(snapshot)

    import spark.implicits._

    filterFileList(
      table_info.range_partition_schema,
      allPartitions,
      partitionFilters).as[PartitionFilterInfo].collect()
  }


  def filesForScan(snapshot: Snapshot,
                   filters: Seq[Expression]): Array[DataFileInfo] = {
    val partitionArray = snapshot.getPartitionInfoArray
    if (filters.length < 1) {
      DataOperation.getTableDataInfo(partitionArray)
    } else {
      val partitionRangeValues = partitionsForScan(snapshot, filters).map(_.range_value).toSet
      val partitionInfo = partitionArray.filter(p => partitionRangeValues.contains(p.range_value))
      DataOperation.getTableDataInfo(partitionInfo)
    }
  }

  def filterFileList(partitionSchema: StructType,
                     files: Seq[DataFileInfo],
                     partitionFilters: Seq[Expression]): Seq[DataFileInfo] = {
    val spark = SparkSession.active
    import spark.implicits._
    val partitionsMatched = filterFileList(partitionSchema,
      files.map(f => PartitionFilterInfo(
        f.range_partitions,
        MetaUtils.getPartitionMapFromKey(f.range_partitions),
        0
      )).toDF,
      partitionFilters).as[PartitionFilterInfo].collect()
    files.filter(f => partitionsMatched.exists(p => p.range_value == f.range_partitions))
  }

  /**
    * Filters the given [[Dataset]] by the given `partitionFilters`, returning those that match.
    *
    * @param files            The active files, which contains the partition value
    *                         information
    * @param partitionFilters Filters on the partition columns
    */
  def filterFileList(partitionSchema: StructType,
                     files: DataFrame,
                     partitionFilters: Seq[Expression]): DataFrame = {
    val rewrittenFilters = rewritePartitionFilters(
      partitionSchema,
      files.sparkSession.sessionState.conf.resolver,
      partitionFilters)
    val columnFilter = new Column(rewrittenFilters.reduceLeftOption(And).getOrElse(Literal(true)))
    files.filter(columnFilter)
  }

  /**
    * Rewrite the given `partitionFilters` to be used for filtering partition values.
    * We need to explicitly resolve the partitioning columns here because the partition columns
    * are stored as keys of a Map type instead of attributes in the DataFileInfo schema (below) and thus
    * cannot be resolved automatically.
    * e.g. (cast('range_partitions.zc as string) = ff)
    *
    * @param partitionFilters        Filters on the partition columns
    * @param partitionColumnPrefixes The path to the `partitionValues` column, if it's nested
    */
  def rewritePartitionFilters(partitionSchema: StructType,
                              resolver: Resolver,
                              partitionFilters: Seq[Expression],
                              partitionColumnPrefixes: Seq[String] = Nil): Seq[Expression] = {
    partitionFilters.map(_.transformUp {
      case a: Attribute =>
        // If we have a special column name, e.g. `a.a`, then an UnresolvedAttribute returns
        // the column name as '`a.a`' instead of 'a.a', therefore we need to strip the backticks.
        val unquoted = a.name.stripPrefix("`").stripSuffix("`")
        val partitionCol = partitionSchema.find { field => resolver(field.name, unquoted) }
        partitionCol match {
          case Some(StructField(name, dataType, _, _)) =>
            Cast(
              UnresolvedAttribute(partitionColumnPrefixes ++ Seq("range_partitions", name)),
              dataType)
          case None =>
            // This should not be able to happen, but the case was present in the original code so
            // we kept it to be safe.
            UnresolvedAttribute(partitionColumnPrefixes ++ Seq("range_partitions", a.name))
        }
    })
  }


}
