// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.catalog

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_PARTITION_DESC_KV_DELIM, LAKESOUL_RANGE_PARTITION_SPLITTER}
import com.dmetasoul.lakesoul.meta.{PartitionInfoScala, SparkMetaVersion}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.lakesoul._
import org.apache.spark.sql.lakesoul.commands.WriteIntoTable
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.{LakeSoulDataSource, LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import java.util
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._
import scala.collection.mutable

case class LakeSoulTableV2(spark: SparkSession,
                           path_orig: Path,
                           catalogTable: Option[CatalogTable] = None,
                           tableIdentifier: Option[String] = None,
                           userDefinedFileIndex: Option[LakeSoulFileIndexV2] = None,
                           var mergeOperatorInfo: Option[Map[String, String]] = None)
  extends Table with SupportsWrite with SupportsRead with SupportsPartitionManagement {

  val path: Path = SparkUtil.makeQualifiedTablePath(path_orig)

  val namespace: String =
    tableIdentifier match {
      case None => LakeSoulCatalog.showCurrentNamespace().mkString(".")
      case Some(tableIdentifier) =>
        val idx = tableIdentifier.lastIndexOf('.')
        if (idx == -1) {
          LakeSoulCatalog.showCurrentNamespace().mkString(".")
        } else {
          tableIdentifier.substring(0, idx)
        }
    }


  private lazy val (rootPath, partitionFilters) = {
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (SparkUtil.makeQualifiedTablePath(new Path(catalogTable.get.location)), Nil)
    } else {
      LakeSoulDataSource.parsePathIdentifier(spark, path.toString)
    }
  }

  // The loading of the SnapshotManagement is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val snapshotManagement: SnapshotManagement = SnapshotManagement(rootPath, namespace)

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .orElse(tableIdentifier)
    .getOrElse(s"lakesoul.`${snapshotManagement.table_path}`")

  private lazy val snapshot: Snapshot = snapshotManagement.snapshot

  private val mapTablePartitionSpec: util.Map[InternalRow, PartitionInfoScala] =
    new ConcurrentHashMap[InternalRow, PartitionInfoScala]()

  override def schema(): StructType =
    StructType(snapshot.getTableInfo.data_schema ++ snapshot.getTableInfo.range_partition_schema)

  private lazy val dataSchema: StructType = snapshot.getTableInfo.data_schema

  private lazy val fileIndex: LakeSoulFileIndexV2 = {
    if (userDefinedFileIndex.isDefined) {
      userDefinedFileIndex.get
    } else {
      DataSoulFileIndexV2(spark, snapshotManagement)
    }
  }

  override def partitioning(): Array[Transform] = {
    snapshot.getTableInfo.range_partition_columns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): java.util.Map[String, String] = {
    val base = new java.util.HashMap[String, String]()
    snapshot.getTableInfo.configuration.foreach { case (k, v) =>
      if (k != "path") {
        base.put(k, v)
      }
    }
    base.put(TableCatalog.PROP_PROVIDER, "lakesoul")
    base.put(TableCatalog.PROP_LOCATION, CatalogUtils.URIToString(path.toUri))
    //    Option(snapshot.getTableInfo.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    base
  }

  override def capabilities(): java.util.Set[TableCapability] = {
    var caps = Set(
      BATCH_READ, V1_BATCH_WRITE, OVERWRITE_DYNAMIC,
      OVERWRITE_BY_FILTER, TRUNCATE, MICRO_BATCH_READ
    )
    if (spark.conf.get(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE)) {
      caps += ACCEPT_ANY_SCHEMA
    }
    caps.asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): LakeSoulScanBuilder = {
    if (mergeOperatorInfo.getOrElse(Map.empty[String, String]).nonEmpty) {
      assert(
        snapshot.getTableInfo.hash_partition_columns.nonEmpty,
        "Merge operator should be used with hash partitioned table")
      val fields = schema().fieldNames
      mergeOperatorInfo.get.map(_._1.replaceFirst(LakeSoulUtils.MERGE_OP_COL, ""))
        .foreach(info => {
          if (!fields.contains(info)) {
            throw LakeSoulErrors.useMergeOperatorForNonLakeSoulTableField(info)
          }
        })
    }
    val newOptions = options.asCaseSensitiveMap().asScala ++
      mergeOperatorInfo.getOrElse(Map.empty[String, String])
    LakeSoulScanBuilder(spark, fileIndex, schema(), dataSchema,
      new CaseInsensitiveStringMap(newOptions.asJava), snapshot.getTableInfo)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoTableBuilder(snapshotManagement, info.options)
  }

  /**
    * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
    * paths.
    */
  def toBaseRelation: BaseRelation = {
    val partitionPredicates = LakeSoulDataSource.verifyAndCreatePartitionFilters(
      path.toString, snapshotManagement.snapshot, partitionFilters)
    SparkUtil.createRelation(partitionPredicates, snapshotManagement, spark)
  }

  override def partitionSchema(): StructType = {
    snapshot.getTableInfo.range_partition_schema
  }

  override def createPartition(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException(
      "Cannot create partition: partitions are created implicitly when inserting new rows into LakeSoul tables")
  }

  override def dropPartition(ident: InternalRow): Boolean = {
    if (mapTablePartitionSpec.containsKey(ident)) {
      val partitionInfoScala = mapTablePartitionSpec.get(ident)
      val deleteFilePaths = SparkMetaVersion.dropPartitionInfoByRangeId(partitionInfoScala.table_id, partitionInfoScala.range_value)
      if (null != deleteFilePaths && deleteFilePaths.length > 0) {
        val sessionHadoopConf = SparkSession.active.sessionState.newHadoopConf()
        val fs = path.getFileSystem(sessionHadoopConf)
        for (item <- deleteFilePaths) {
          fs.delete(new Path(item), true)
        }
      }
      mapTablePartitionSpec.remove(ident)
      true
    } else {
      false
    }
  }

  override def replacePartitionMetadata(ident: InternalRow, properties: util.Map[String, String]): Unit = {
    throw new UnsupportedOperationException(
      "Cannot replace partition metadata: LakeSoul table partitions do not support metadata")
  }

  override def loadPartitionMetadata(ident: InternalRow): util.Map[String, String] = {
    throw new UnsupportedOperationException(
      "Cannot load partition metadata: LakeSoul table partitions do not support metadata")
  }

  override def listPartitionIdentifiers(names: Array[String], ident: InternalRow): Array[InternalRow] = {
    assert(names.length == ident.numFields,
      s"Number of partition names (${names.length}) must be equal to " +
        s"the number of partition values (${ident.numFields}).")
    val schema = partitionSchema
    assert(names.forall(fieldName => schema.fieldNames.contains(fieldName)),
      s"Some partition names ${names.mkString("[", ", ", "]")} don't belong to " +
        s"the partition schema '${schema.sql}'.")
    val indexes = names.map(schema.fieldIndex)
    val dataTypes = names.map(schema(_).dataType)
    val currentRow = new GenericInternalRow(new Array[Any](names.length))
    val partitionInfoArray = snapshot.getPartitionInfoArray

    partitionInfoArray.foreach(partition => {
      val range_value = partition.range_value
      val map = range_value.split(LAKESOUL_RANGE_PARTITION_SPLITTER).map(p => {
        val strings = p.split(LAKESOUL_PARTITION_DESC_KV_DELIM)
        strings(0) -> strings(1)
      }).toMap
      val row = ResolvePartitionSpec.convertToPartIdent(map, schema)
      mapTablePartitionSpec.put(row, partition)
    })

    val ss = mapTablePartitionSpec.keySet().asScala.filter { key =>
      for (i <- 0 until names.length) {
        currentRow.values(i) = key.get(indexes(i), dataTypes(i))
      }
      currentRow == ident
    }.toArray
    ss
  }

}

private class WriteIntoTableBuilder(snapshotManagement: SnapshotManagement,
                                    writeOptions: CaseInsensitiveStringMap)
  extends WriteBuilder with SupportsOverwrite with SupportsTruncate {

  private var forceOverwrite = false

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteIntoTableBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw new AnalysisException(
        "You can't use replaceWhere in conjunction with an overwrite by filter")
    }
    options.put("replaceWhere", LakeSoulSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  // use v1write temporarily
  override def build(): V1Write = {
    new V1Write {
      override def toInsertableRelation: InsertableRelation =
        (data: DataFrame, overwrite: Boolean) => {
          val session = data.sparkSession

          WriteIntoTable(
            snapshotManagement,
            if (forceOverwrite || overwrite) SaveMode.Overwrite else SaveMode.Append,
            new LakeSoulOptions(options.toMap, session.sessionState.conf),
            snapshotManagement.snapshot.getTableInfo.configuration,
            data).run(session)
        }
    }
  }
}
