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

import com.dmetasoul.lakesoul.meta.{DataOperation, MetaUtils}
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, Expression, Literal}
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionFilterInfo, SparkUtil}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object PartitionFilter {

  def partitionsForScan(snapshot: Snapshot, filters: Seq[Expression]): Seq[PartitionFilterInfo] = {
    val table_info = snapshot.getTableInfo

    val partitionFilters = filters.flatMap { filter =>
      LakeSoulUtils.splitMetadataAndDataPredicates(filter, table_info.range_partition_columns, SparkUtil.spark)._1
    }
    val allPartitions = SparkUtil.allPartitionFilterInfoDF(snapshot)

    import SparkUtil.spark.implicits._

    filterFileList(
      table_info.range_partition_schema,
      allPartitions,
      partitionFilters).as[PartitionFilterInfo].collect()
  }


  def filesForScan(snapshot: Snapshot,
                   filters: Seq[Expression]): Array[DataFileInfo] = {
    val partitionRangeValues = partitionsForScan(snapshot, filters).map(_.range_value)
    val partitionInfo = snapshot.getPartitionInfoArray.filter(p => partitionRangeValues.contains(p.range_value))
    DataOperation.getTableDataInfo(partitionInfo)
  }

  def filterFileList(partitionSchema: StructType,
                     files: Seq[DataFileInfo],
                     partitionFilters: Seq[Expression]): Seq[DataFileInfo] = {
    import SparkUtil.spark.implicits._
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
