// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.sources

import com.dmetasoul.lakesoul.meta.{DataFileInfo, MetaCommit, MetaUtils, SparkMetaVersion}

import java.util.Locale
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.{TableIdentifier, expressions}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.lakesoul.commands.WriteIntoTable
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo}
import org.apache.spark.sql.lakesoul.{LakeSoulOptions, SnapshotManagement}
import org.apache.spark.sql.types.StructType

object LakeSoulSourceUtils {

  val NAME = "lakesoul"
  val SOURCENAME = "lakesoul"

  def isLakeSoulDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME
  }

  def isLakeSoulTableExists(path: String): Boolean = {
    val table_name = path
    SparkMetaVersion.isTableExists(table_name)
  }

  def isLakeSoulShortTableNameExists(shortName: String): Boolean = {
    SparkMetaVersion.isShortTableNameExists(shortName)._1
  }

  def isLakeSoulShortTableNameExists(shortName: String, namespace: String): Boolean = {
    SparkMetaVersion.isShortTableNameExists(shortName, namespace)._1
  }

  /** Check whether this table is a lakesoul table based on information from the Catalog. */
  def isLakeSoulTable(provider: Option[String]): Boolean = {
    provider.exists(isLakeSoulDataSourceName)
  }

  def getLakeSoulPathByTableIdentifier(table: TableIdentifier): Option[String] = {
    SparkMetaVersion.isShortTableNameExists(table.table,
      table.database.getOrElse(LakeSoulCatalog.showCurrentNamespace()(0))) match {
      case (true, path) => Some(path)
      case _ => None
    }
  }

  /** Creates Spark literals from a value exposed by the public Spark API. */
  private def createLiteral(value: Any): expressions.Literal = value match {
    case v: String => expressions.Literal.create(v)
    case v: Int => expressions.Literal.create(v)
    case v: Byte => expressions.Literal.create(v)
    case v: Short => expressions.Literal.create(v)
    case v: Long => expressions.Literal.create(v)
    case v: Double => expressions.Literal.create(v)
    case v: Float => expressions.Literal.create(v)
    case v: Boolean => expressions.Literal.create(v)
    case v: java.sql.Date => expressions.Literal.create(v)
    case v: java.sql.Timestamp => expressions.Literal.create(v)
    case v: BigDecimal => expressions.Literal.create(v)
  }

  /** Translates the public Spark Filter APIs into Spark internal expressions. */
  def translateFilters(filters: Array[Filter]): Expression = filters.map {
    case sources.EqualTo(attribute, value) =>
      expressions.EqualTo(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.EqualNullSafe(attribute, value) =>
      expressions.EqualNullSafe(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThan(attribute, value) =>
      expressions.GreaterThan(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThanOrEqual(attribute, value) =>
      expressions.GreaterThanOrEqual(
        UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThan(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThanOrEqual(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.In(attribute, values) =>
      expressions.In(UnresolvedAttribute(attribute), values.map(createLiteral))
    case sources.IsNull(attribute) => expressions.IsNull(UnresolvedAttribute(attribute))
    case sources.IsNotNull(attribute) => expressions.IsNotNull(UnresolvedAttribute(attribute))
    case sources.Not(otherFilter) => expressions.Not(translateFilters(Array(otherFilter)))
    case sources.And(filter1, filter2) =>
      expressions.And(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.Or(filter1, filter2) =>
      expressions.Or(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.StringStartsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"$value%"))
    case sources.StringEndsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%$value"))
    case sources.StringContains(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%$value%"))
    case sources.AlwaysTrue() => expressions.Literal.TrueLiteral
    case sources.AlwaysFalse() => expressions.Literal.FalseLiteral
  }.reduce(expressions.And)

}

case class LakeSoulBaseRelation(files: Seq[DataFileInfo],
                                snapshotManagement: SnapshotManagement)(val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  lazy val tableInfo: TableInfo = snapshotManagement.snapshot.getTableInfo

  override def schema: StructType = {
    tableInfo.schema
  }


  /**
    * Build the RDD to scan rows. todo: True predicates filter
    *
    * @param requiredColumns columns that are being requested by the requesting query
    * @param filters         filters that are being applied by the requesting query
    * @return RDD will all the results from lakesoul
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    val predicts = filters.length match {
      case 0 => expressions.Literal(true)
      case _ => LakeSoulSourceUtils.translateFilters(filters)
    }

    SparkUtil
      .createDataFrame(files, requiredColumns, snapshotManagement, Option(predicts))
      .filter(Column(predicts))
      .select(requiredColumns.map(col): _*)
      .rdd
  }


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    WriteIntoTable(
      snapshotManagement,
      mode,
      new LakeSoulOptions(Map.empty[String, String], sqlContext.conf),
      Map.empty,
      data).run(sparkSession)
  }


  /**
    * Returns the string representation of this LakeSoulRelation
    *
    * @return LakeSoul + tableName of the relation
    */
  override def toString(): String = {
    "LakeSoul " + tableInfo.table_path_s.get
  }


}
