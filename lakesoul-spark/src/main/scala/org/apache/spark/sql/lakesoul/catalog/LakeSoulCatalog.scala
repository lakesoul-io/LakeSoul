// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.catalog

import com.dmetasoul.lakesoul.meta.SparkMetaVersion
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{DataSource, DataSourceUtils, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulConfig
import org.apache.spark.sql.lakesoul.commands._
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps

/**
  * A Catalog extension which can properly handle the interaction between the HiveMetaStore and
  * lakesoul tables. It delegates all operations DataSources other than lakesoul to the SparkCatalog.
  */
class LakeSoulCatalog(val spark: SparkSession) extends TableCatalog
  with StagingTableCatalog
  with SupportsPathIdentifier
  with SupportsNamespaces {

  def this() = {
    this(SparkSession.active)
  }

  var catalogName = ""

  override def name(): String = catalogName

  /**
    * Creates a lakesoul table
    *
    * @param ident       The identifier of the table
    * @param schema      The schema of the table
    * @param partitions  The partition transforms for the table
    * @param properties  The table properties. Right now it also includes write options for backwards
    *                    compatibility
    * @param sourceQuery A query if this CREATE request came from a CTAS or RTAS
    * @param operation   The specific table creation mode, whether this is a Create/Replace/Create or
    *                    Replace
    */
  private def createLakeSoulTable(ident: Identifier,
                                  schema: StructType,
                                  partitions: Array[Transform],
                                  properties: util.Map[String, String],
                                  sourceQuery: Option[LogicalPlan],
                                  operation: TableCreationModes.CreationMode): Table = {
    // These two keys are properties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    if (!LakeSoulCatalog.namespaceExists(ident.namespace())) {
      throw new NoSuchNamespaceException(ident.toString)
    }
    val tableProperties = properties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case "path" => false
      case _ => true
    }
    // START: This entire block until END is a copy-paste from V2SessionCatalog
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    val isByPath = isPathIdentifier(ident)
    val (location, existingLocation) = if (isByPath) {
      (Option(ident.name()),
        if (LakeSoulSourceUtils.isLakeSoulTableExists(ident.name())) Some(ident.name()) else None)
    } else {
      (Option(properties.get("location")),
        Option(SparkMetaVersion.isShortTableNameExists(ident.name(), ident.namespace().mkString("."))._2))
    }
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED

    val tableDesc = new CatalogTable(
      identifier = TableIdentifier(ident.name(), ident.namespace().lastOption),
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("lakesoul"),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      comment = Option(properties.get("comment")))
    // END: copy-paste from the super method finished.

    val withDb = verifyTableAndSolidify(tableDesc, None)

    DataSourceUtils.checkFieldNames(new ParquetFileFormat(), tableDesc.schema)
    CreateTableCommand(
      withDb,
      existingLocation.map(SparkUtil.makeQualifiedPath(_).toString),
      operation.mode,
      sourceQuery,
      operation,
      tableByPath = isByPath,
      this).run(spark)

    loadTable(ident)
  }

  override def loadTable(identifier: Identifier): Table = {
    val ident = identifier.namespace() match {
      case Array() => Identifier.of(LakeSoulCatalog.showCurrentNamespace(), identifier.name())
      case _ => identifier
    }
    if (isPathIdentifier(ident)) {
      val tableInfo = SparkMetaVersion.getTableInfo(ident.namespace().mkString("."), ident.name())
      if (tableInfo == null) {
        throw new NoSuchTableException(ident)
      }
      LakeSoulTableV2(
        spark,
        new Path(ident.name()),
        None,
        Some(Identifier.of(ident.namespace(), tableInfo.short_table_name.getOrElse(tableInfo.table_path.toString)).toString)
      )
    } else if (isNameIdentifier(ident)) {
      val tablePath = SparkMetaVersion.getTablePathFromShortTableName(ident.name, ident.namespace().mkString("."))
      if (tablePath == null) {
        throw new NoSuchTableException(ident)
      }
      LakeSoulTableV2(
        spark,
        new Path(tablePath),
        None,
        Some(ident.toString)
      )
    } else {
      throw new NoSuchTableException(ident)
    }
  }

  def getTableLocation(ident: Identifier): Option[String] = {
    try {
      val table = loadTable(ident)
      Option(table.properties().get("location"))
    } catch {
      case _: NoSuchNamespaceException | _: NoSuchTableException => None
    }
  }

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider"))
      .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): Table = {
    if (LakeSoulSourceUtils.isLakeSoulDataSourceName(getProvider(properties))) {
      createLakeSoulTable(
        ident, schema, partitions, properties, sourceQuery = None, TableCreationModes.Create)
    } else {
      throw LakeSoulErrors.analysisException(
        s"Not a lakesoul table: ${getProvider(properties)}", plan = None)
    }
  }

  override def stageReplace(
                             ident: Identifier,
                             schema: StructType,
                             partitions: Array[Transform],
                             properties: util.Map[String, String]): StagedTable = {
    if (LakeSoulSourceUtils.isLakeSoulDataSourceName(getProvider(properties))) {
      throw LakeSoulErrors.operationNotSupportedException("replaceTable")
    } else {
      dropTable(ident)
      BestEffortStagedTable(
        ident,
        createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreateOrReplace(
                                     ident: Identifier,
                                     schema: StructType,
                                     partitions: Array[Transform],
                                     properties: util.Map[String, String]): StagedTable = {
    val iden = ident.namespace() match {
      case Array(_) => ident
      case Array() => Identifier.of(LakeSoulCatalog.showCurrentNamespace(), ident.name())
    }
    if (LakeSoulSourceUtils.isLakeSoulDataSourceName(getProvider(properties))) {
      new StagedLakeSoulTableV2(
        iden, schema, partitions, properties, TableCreationModes.CreateOrReplace)
    } else {
      try dropTable(iden) catch {
        case _: NoSuchTableException => // this is fine
      }
      BestEffortStagedTable(
        iden,
        createTable(iden, schema, partitions, properties),
        this)
    }
  }

  override def stageCreate(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): StagedTable = {
    val iden = ident.namespace() match {
      case Array(_) => ident
      case Array() => Identifier.of(LakeSoulCatalog.showCurrentNamespace(), ident.name())
    }
    if (LakeSoulSourceUtils.isLakeSoulDataSourceName(getProvider(properties))) {
      new StagedLakeSoulTableV2(iden, schema, partitions, properties, TableCreationModes.Create)
    } else {
      BestEffortStagedTable(
        iden,
        createTable(iden, schema, partitions, properties),
        this)
    }
  }

  // Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, col, sortCol) =>
        bucketSpec = Some(BucketSpec(numBuckets, col.map(_.fieldNames.mkString(".")),
          sortCol.map(_.fieldNames.mkString("."))))

      case _ =>
        throw LakeSoulErrors.operationNotSupportedException(s"Partitioning by expressions")
    }

    (identityCols, bucketSpec)
  }

  /** Performs checks on the parameters provided for table creation for a LakeSoul table. */
  private def verifyTableAndSolidify(tableDesc: CatalogTable,
                                     query: Option[LogicalPlan]): CatalogTable = {

    if (tableDesc.bucketSpec.isDefined) {
      throw LakeSoulErrors.operationNotSupportedException("Bucketing", Some(tableDesc.identifier))
    }

    val ori_schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    val schema = StructType(ori_schema.map {
      case StructField(name, dataType, nullable, metadata) =>
        if (tableDesc.partitionColumnNames.contains(name)) {
          StructField(name, dataType, nullable = false, metadata)
        } else {
          StructField(name, dataType, nullable, metadata)
        }
    })


    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // lakesoul is case insensitive

    val validatedConfigurations = LakeSoulConfig.validateConfigurations(tableDesc.properties)

    val db = tableDesc.identifier.database.getOrElse(spark.sessionState.catalogManager.currentNamespace(0))
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = validatedConfigurations)
  }

  private class StagedLakeSoulTableV2(
                                       ident: Identifier,
                                       override val schema: StructType,
                                       val partitions: Array[Transform],
                                       override val properties: util.Map[String, String],
                                       operation: TableCreationModes.CreationMode) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = properties.asScala.toMap

    override def commitStagedChanges(): Unit = {
      createLakeSoulTable(
        ident,
        schema,
        partitions,
        writeOptions.asJava,
        asSelectQuery.map(_.queryExecution.analyzed),
        operation)
    }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
      // TODO: We now pass both properties and options into CreateTableCommand, because
      // it wasn't supported in the initial APIs, but with DFWriterV2, we should actually separate
      // them
      val combinedProps = info.options.asCaseSensitiveMap().asScala ++ properties.asScala
      writeOptions = combinedProps.toMap
      new LakeSoulV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a lakesoul table.
     */
    private class LakeSoulV1WriteBuilder extends WriteBuilder {
      override def build(): V1Write = {
        new V1Write {
          override def toInsertableRelation: InsertableRelation =
            (data: DataFrame, overwrite: Boolean) => {
              asSelectQuery = Option(data)
            }
        }
      }
    }

  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident) match {
      case lakeSoulTable: LakeSoulTableV2 => lakeSoulTable
      case _ => throw new NoSuchTableException(ident)
    }

    // We group the table changes by their type, since lakesoul applies each in a separate action.
    // We also must define an artificial type for SetLocation, since data source V2 considers
    // location just another property but it's special in catalog tables.
    class SetLocation {}
    val grouped = changes.groupBy {
      case s: SetProperty if s.property() == "location" => classOf[SetLocation]
      case c => c.getClass
    }

    val columnUpdates = new mutable.HashMap[Seq[String], (StructField, Option[ColumnPosition])]()

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        AlterTableAddColumnsCommand(
          table,
          newColumns.map(_.asInstanceOf[AddColumn])).run(spark)

      case (t, newProperties) if t == classOf[SetProperty] =>
        AlterTableSetPropertiesCommand(
          table,
          LakeSoulConfig.validateConfigurations(
            newProperties.asInstanceOf[Seq[SetProperty]].map { prop =>
              prop.property() -> prop.value()
            }.toMap)
        ).run(spark)

      case (t, oldProperties) if t == classOf[RemoveProperty] =>
        AlterTableUnsetPropertiesCommand(
          table,
          oldProperties.asInstanceOf[Seq[RemoveProperty]].map(_.property()),
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case (t, columnChanges) if classOf[ColumnChange].isAssignableFrom(t) =>
        def getColumn(fieldNames: Seq[String]): (StructField, Option[ColumnPosition]) = {
          columnUpdates.getOrElseUpdate(fieldNames, {
            val schema = table.snapshotManagement.snapshot.getTableInfo.schema
            val colName = UnresolvedAttribute(fieldNames).name
            val fieldOpt = schema.findNestedField(fieldNames, includeCollections = true,
              spark.sessionState.conf.resolver)
              .map(_._2)
            val field = fieldOpt.getOrElse {
              throw new AnalysisException(
                s"Couldn't find column $colName in:\n${schema.treeString}")
            }
            field -> None
          })
        }

        columnChanges.foreach {
          case comment: UpdateColumnComment =>
            val field = comment.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.withComment(comment.newComment()) -> pos

          case dataType: UpdateColumnType =>
            val field = dataType.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(dataType = dataType.newDataType()) -> pos

          case position: UpdateColumnPosition =>
            val field = position.fieldNames()
            val (oldField, _) = getColumn(field)
            columnUpdates(field) = oldField -> Option(position.position())

          case nullability: UpdateColumnNullability =>
            val field = nullability.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(nullable = nullability.nullable()) -> pos

          case rename: RenameColumn =>
            val field = rename.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(name = rename.newName()) -> pos

          case other =>
            throw LakeSoulErrors.operationNotSupportedException("Unrecognized column change " + other.getClass)
        }

      case (t, _) if t == classOf[SetLocation] =>
        throw LakeSoulErrors.operationNotSupportedException("ALTER TABLE xxx SET LOCATION '/xxx'")
    }

    columnUpdates.foreach { case (fieldNames, (newField, newPositionOpt)) =>
      AlterTableChangeColumnCommand(
        table,
        fieldNames.dropRight(1),
        fieldNames.last,
        newField,
        newPositionOpt).run(spark)
    }

    loadTable(ident)
  }

  // We want our catalog to handle lakesoul, therefore for other data sources that want to be
  // created, we just have this wrapper StagedTable to only drop the table if the commit fails.
  private case class BestEffortStagedTable(
                                            ident: Identifier,
                                            table: Table,
                                            catalog: TableCatalog) extends StagedTable with SupportsWrite {
    override def abortStagedChanges(): Unit = catalog.dropTable(ident)

    override def commitStagedChanges(): Unit = {}

    // Pass through
    override def name(): String = table.name()

    override def schema(): StructType = table.schema()

    override def partitioning(): Array[Transform] = table.partitioning()

    override def capabilities(): util.Set[TableCapability] = table.capabilities()

    override def properties(): util.Map[String, String] = table.properties()

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = table match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ => throw new AnalysisException(s"Table implementation does not support writes: $name")
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)) {
      LakeSoulTable.forPath(ident.name()).dropTable()
    } else if (isNameIdentifier(ident)) {
      LakeSoulTable.forName(ident.name(), ident.namespace().mkString(".")).dropTable()
    } else {
      false
    }
  }

  override def listTables(namespaces: Array[String]): Array[Identifier] = {
    LakeSoulCatalog.listTables(namespaces)
  }

  //=============
  // Namespace
  //=============

  override def createNamespace(namespaces: Array[String], metadata: util.Map[String, String]): Unit = {
    LakeSoulCatalog.createNamespace(namespaces)
  }

  override def listNamespaces(): Array[Array[String]] = {
    LakeSoulCatalog.listNamespaces()
  }

  override def defaultNamespace(): Array[String] = {
    LakeSoulCatalog.currentDefaultNamespace
  }


  override def namespaceExists(namespace: Array[String]): Boolean = {
    LakeSoulCatalog.namespaceExists(namespace)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    LakeSoulCatalog.dropNamespace(namespace)
  }


  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw LakeSoulErrors.operationNotSupportedException("LakeSoul currently doesn't support rename table")
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if LakeSoulCatalog.namespaceExists(Array(db)) =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        if (!LakeSoulCatalog.namespaceExists(namespace)) throw new NoSuchNamespaceException(db)
        mutable.HashMap[String, String]().asJava

      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    throw LakeSoulErrors.operationNotSupportedException("LakeSoul currently doesn't support rename namespace")
  }


  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name
  }
}

/**
  * A trait for handling table access through lakesoul.`/some/path`. This is a stop-gap solution
  * until PathIdentifiers are implemented in Apache Spark.
  */
trait SupportsPathIdentifier extends TableCatalog {
  self: LakeSoulCatalog =>

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  private def hasLakeSoulNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && LakeSoulCatalog.namespaceExists(ident.namespace())
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasLakeSoulNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  protected def isNameIdentifier(ident: Identifier): Boolean = {
    ident.namespace() match {
      case Array() =>
        SparkMetaVersion.isShortTableNameExists(ident.name())._1
      case _ =>
        SparkMetaVersion.isShortTableNameExists(ident.name(), ident.namespace().mkString("."))._1
    }
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(Identifier.of(table.identifier.database.toArray, table.identifier.table))
  }

  override def tableExists(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)) {
      LakeSoulSourceUtils.isLakeSoulTableExists(ident.name())
    } else if (isNameIdentifier(ident)) {
      LakeSoulSourceUtils.isLakeSoulShortTableNameExists(ident.name(), ident.namespace().mkString("."))
    } else {
      false
    }
  }
}

object LakeSoulCatalog {
  //===========
  // namespaces
  //===========

  var currentDefaultNamespace: Array[String] = Array("default")

  def listTables(): Array[Identifier] = {
    listTables(currentDefaultNamespace)
  }

  def listTables(namespaces: Array[String]): Array[Identifier] = {
    SparkMetaVersion.listTables(namespaces).asScala.map(tablePath => {
      val tableInfo = SparkMetaVersion.getTableInfo(tablePath)
      Identifier.of(namespaces, tableInfo.short_table_name.getOrElse(tableInfo.table_path.toString))
    }).toArray
  }

  def createNamespace(namespace: Array[String]): Unit = {
    SparkMetaVersion.createNamespace(namespace.mkString("."))
  }


  def useNamespace(namespace: Array[String]): Unit = {
    currentDefaultNamespace = namespace
  }

  def showCurrentNamespace(): Array[String] = {
    currentDefaultNamespace = SparkSession.active.sessionState.catalogManager.currentNamespace
    currentDefaultNamespace
  }

  def isCurrentNamespace(namespace: Array[String]): Boolean = {
    currentDefaultNamespace.mkString(".") == namespace.mkString(".")
  }

  def listNamespaces(): Array[Array[String]] = {
    SparkMetaVersion.listNamespaces().map(namespace => namespace.split("\\."))
  }

  def namespaceExists(namespace: Array[String]): Boolean = {
    SparkMetaVersion.isNamespaceExists(namespace.mkString("."))
  }

  def dropNamespace(namespace: Array[String]): Boolean = {
    SparkMetaVersion.dropNamespaceByNamespace(namespace.mkString("."))
    if (isCurrentNamespace(namespace)) {
      useNamespace(Array("default"))
    }
    true
  }

  //  just for test
  def cleanMeta(): Unit = {
    SparkMetaVersion.cleanMeta()
  }

  final val CATALOG_NAME: String = "lakesoul"
}
