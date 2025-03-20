// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{DBConfig, DBUtil, DataFileInfo, DataOperation, MetaUtils, PartitionInfoScala, SparkMetaVersion}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, Cast, Equality, Expression, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.lakesoul.utils.{PartitionFilterInfo, SparkUtil, TableInfo}
import org.apache.spark.sql.types.{BooleanType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import java.util.UUID
import scala.collection.JavaConverters.mapAsScalaMapConverter

object PartitionFilter extends Logging {

  private def filterFromAllPartitionInfo(snapshot: Snapshot, table_info: TableInfo, filters: Seq[Expression]): Seq[PartitionFilterInfo] = {
    val spark = SparkSession.active
    val allPartitions = SparkUtil.allPartitionFilterInfoDF(snapshot)

    import spark.implicits._

    val filteredParts = filterFileList(
      table_info.range_partition_schema,
      allPartitions,
      filters).as[PartitionFilterInfo].collect()
    filteredParts.foreach(p => {
      snapshot.recordPartitionInfoRead(PartitionInfoScala(
        p.table_id,
        p.range_value,
        p.read_version,
        p.read_files.map(UUID.fromString),
        p.expression,
        p.commit_op
      ))
    })
    filteredParts
  }

  def partitionsForScan(snapshot: Snapshot, filters: Seq[Expression]): Seq[PartitionFilterInfo] = {
    val table_info = snapshot.getTableInfo

    val spark = SparkSession.active
    val partitionFilters = filters.flatMap { filter =>
      LakeSoulUtils.splitMetadataAndDataPredicates(filter, table_info.range_partition_columns, spark)._1
    }
    snapshot.getPartitionInfoFromCache(partitionFilters) match {
      case Some(info) => info.toSeq
      case None =>
        val infos = if (partitionFilters.isEmpty) {
          logInfo(s"no partition filter for table ${table_info.table_path}")
          filterFromAllPartitionInfo(snapshot, table_info, partitionFilters)
        } else {
          val equalExprs = partitionFilters.collect {
            case Equality(UnresolvedAttribute(nameParts), lit@Literal(_, _)) =>
              val colName = nameParts.last
              val colValue = lit.toString()
              colName -> colValue
            case Equality(NamedExpression(n), lit@Literal(_, _)) =>
              val colName = n._1
              val colValue = lit.toString()
              colName -> colValue
          }.toMap
          // remove useless isnotnull/true expr
          val newPartitionFilters = partitionFilters.filter({
            case IsNotNull(AttributeReference(name, _, _, _)) => !equalExprs.contains(name)
            case Literal(v, BooleanType) if v.asInstanceOf[Boolean] => false
            case _ => true
          })

          if (table_info.range_partition_columns.nonEmpty && table_info.range_partition_columns.forall(p => {
            equalExprs.contains(p)
          })) {
            // optimize for all partition equality filter
            val partDesc = table_info.range_partition_columns.map(p => {
              val colValue = equalExprs(p)
              s"$p=$colValue"
            }).mkString(DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER)
            val partInfo = SparkMetaVersion.getSinglePartitionInfo(table_info.table_id, partDesc, "")
            logInfo(s"All equality partition filters $equalExprs for table ${table_info.table_path}, using" +
              s" desc $partDesc")
            if (partInfo == null) return Seq.empty
            snapshot.recordPartitionInfoRead(partInfo)
            Seq(PartitionFilterInfo(
              partDesc,
              equalExprs,
              partInfo.version,
              table_info.table_id,
              partInfo.read_files.map(u => u.toString),
              partInfo.expression,
              partInfo.commit_op
            ))
          } else if (equalExprs.size == newPartitionFilters.size) {
            // optimize for partial equality filter (no non-eq filter)
            // using ts query
            val partQuery = equalExprs.map({ case (k, v) => s"$k=$v" }).mkString(" & ")
            logInfo(s"Partial equality partition filters $equalExprs for table ${table_info.table_path}, using" +
              s" query $partQuery")
            SparkMetaVersion.getPartitionInfoByEqFilters(table_info.table_id, partQuery)
              .map(partInfo => {
                PartitionFilterInfo(
                  partInfo.range_value,
                  DBUtil.parsePartitionDesc(partInfo.range_value).asScala.toMap,
                  partInfo.version,
                  table_info.table_id,
                  partInfo.read_files.map(u => u.toString),
                  partInfo.expression,
                  partInfo.commit_op
                )
              })
          } else {
            // non-equality filter, we still need to get all partitions from meta
            logInfo(s"at least one non-equality filter exist $partitionFilters, read all partition info")
            filterFromAllPartitionInfo(snapshot, table_info, partitionFilters)
          }
        }
        snapshot.putPartitionInfoCache(partitionFilters, infos)
        infos
    }
  }


  def filesForScan(snapshot: Snapshot,
                   filters: Seq[Expression]): Array[DataFileInfo] = {
    if (filters.length < 1) {
      val partitionArray = snapshot.getPartitionInfoArray
      DataOperation.getTableDataInfo(partitionArray)
    } else {
      val partitionFiltered = partitionsForScan(snapshot, filters)
      val partitionInfo = partitionFiltered.map(p => {
        PartitionInfoScala(
          p.table_id,
          p.range_value,
          p.read_version,
          p.read_files.map(UUID.fromString),
          p.expression,
          p.commit_op
        )
      }).toArray
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
        0,
        ""
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
