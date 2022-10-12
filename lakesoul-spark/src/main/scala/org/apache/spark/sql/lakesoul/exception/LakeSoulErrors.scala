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

package org.apache.spark.sql.lakesoul.exception

import com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.schema.{Invariant, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.lakesoul.{LakeSoulConfig, LakeSoulOptions}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, StructField, StructType}


object LakeSoulErrors {

  def analysisException(
                         msg: String,
                         line: Option[Int] = None,
                         startPosition: Option[Int] = None,
                         plan: Option[LogicalPlan] = None,
                         cause: Option[Throwable] = None): AnalysisException = {
    new AnalysisException(msg, line, startPosition, plan, cause)
  }

  def formatColumn(colName: String): String = s"`$colName`"

  def formatColumnList(colNames: Seq[String]): String =
    colNames.map(formatColumn).mkString("[", ", ", "]")

  def formatSchema(schema: StructType): String = schema.treeString


  def failedCreateTableException(table_name: String): Throwable = {
    new AnalysisException(
      s"""
         |Error: Failed to create table: $table_name. The associated location ('$table_name') is not empty.
       """.stripMargin)
  }

  def failedCreateTableException(table_name: String, location: String): Throwable = {
    new AnalysisException(
      s"""
         |Error: Failed to create table: $table_name. The associated location ('$location') is not empty.
       """.stripMargin)
  }


  def CompactionException(table_name: String): Throwable = {
    new AnalysisException(
      s"""
         |Error: No delta file for compaction: $table_name.
       """.stripMargin)
  }

  def failedInitTableException(table_name: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to init meta info for table: $table_name,
         |this table may already exists.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def failedAddShortTableNameException(short_table_name: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to add short table name for table: $short_table_name,
         |this table may already exists.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def failedAddFragmentValueException(id: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to add fragment value for id: $id,
         |this uuid may already exists, this is unexpected, please have a check.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def failedAddPartitionVersionException(table_name: String, range_value: String, range_id: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to add partition version for table: $table_name, partition: $range_value,
         |id: $range_id, this partition may already exists.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def failedUpdatePartitionReadVersionException(table_name: String,
                                                range_partition: String,
                                                commit_id: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to update partition read version.
         |Error table: $table_name, partition: $range_partition .
       """.stripMargin.split("\n").mkString(" ").trim,
      commit_id)
  }

  def failedUpdatePartitionUndoLogException(): MetaException = {
    new MetaException(
      s"""
         |Error: Update write_version to partition undo log was failed.
       """.stripMargin.trim)
  }


  def getWriteVersionError(table_name: String,
                           needPartition: String,
                           foundPartitions: String,
                           commit_id: String): MetaException = {
    new MetaException(
      s"""
         |Error: Failed to get write version from meta_info.
         |Partition "$needPartition" was needed, but there were "$foundPartitions" founded.
         |Error table: $table_name.
       """.stripMargin,
      commit_id)
  }

  def commitFailedReachLimit(table_name: String,
                             commit_id: String,
                             limit: Int): MetaException = {
    new MetaException(
      s"""
         |Error: Table `$table_name` was failed to commit for $limit times.
       """.stripMargin,
      commit_id)
  }

  def tableExistsException(table: String): Throwable = {
    new AnalysisException(s"Table $table already exists.")
  }

  def tableNotExistsException(table_path: String): Throwable = {
    new AnalysisException(s"Table $table_path doesn't exist.")
  }

  def schemaNotSetException: Throwable = {
    new AnalysisException(
      "Table schema is not set.  Write data into it or use CREATE TABLE to set the schema.")
  }

  def specifySchemaAtReadTimeException: Throwable = {
    new AnalysisException("LakeSoul does not support specifying the schema at read time.")
  }

  def pathNotSpecifiedException: Throwable = {
    new IllegalArgumentException("'path' is not specified")
  }

  def outputModeNotSupportedException(dataSource: String, outputMode: OutputMode): Throwable = {
    new AnalysisException(
      s"Data source $dataSource is only support $outputMode output mode with hash partition")
  }

  def streamWriteNullTypeException: Throwable = {
    new AnalysisException(
      "LakeSoul doesn't accept NullTypes in the schema for streaming writes.")
  }

  def partitionColumnNotFoundException(colName: String, schema: Seq[Attribute]): Throwable = {
    new AnalysisException(
      s"Partition column ${formatColumn(colName)} not found in schema " +
        s"[${schema.map(_.name).mkString(", ")}]")
  }

  def partitionColumnConflictException(tableCols: String, optionCols: String, partition_type: String): Throwable = {
    new AnalysisException(
      s"$partition_type partition column `$tableCols` was already set when creating table, " +
        s"it conflicts with your partition columns  `$optionCols`.")
  }

  def hashBucketNumConflictException(tableNum: Int, optionNum: Int): Throwable = {
    new AnalysisException(
      s"HashBucketNum was already set to $tableNum when creating table, " +
        s"and conflict with your num $optionNum.")
  }

  def nonPartitionColumnAbsentException(colsDropped: Boolean): Throwable = {
    val msg = if (colsDropped) {
      " Columns which are of NullType have been dropped."
    } else {
      ""
    }
    new AnalysisException(
      s"Data written into table needs to contain at least one non-partitioned column.$msg")
  }

  def illegalLakeSoulOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value '$input' for option '$name', $explain")
  }

  def notNullInvariantException(invariant: Invariant): Throwable = {
    new InvariantViolationException(s"Column ${UnresolvedAttribute(invariant.column).name}" +
      s", which is defined as ${invariant.rule.name}, is missing from the data being " +
      s"written into the table.")
  }

  def ambiguousPartitionColumnException(
                                         columnName: String, colMatches: Seq[StructField]): Throwable = {
    new AnalysisException(
      s"Ambiguous partition column ${formatColumn(columnName)} can be" +
        s" ${formatColumnList(colMatches.map(_.name))}.")
  }

  def unknownConfigurationKeyException(confKey: String): Throwable = {
    new AnalysisException(s"Unknown configuration was specified: $confKey")
  }

  def emptyDataException: Throwable = {
    new AnalysisException(
      "Data used in creating the table doesn't have any columns.")
  }


  def rangePartitionColumnChangeException(): Throwable = {
    new AnalysisException("Range partiton has been changed !")
  }

  def pathAlreadyExistsException(path: Path): Throwable = {
    new AnalysisException(s"$path already exists.")
  }


  def modifyAppendOnlyTableException: Throwable = {
    new UnsupportedOperationException(
      "This table is configured to only allow appends. If you would like to permit " +
        s"updates or deletes, use 'ALTER TABLE <table_name> SET TBLPROPERTIES " +
        s"(${LakeSoulConfig.IS_APPEND_ONLY.key}=false)'.")
  }

  def replaceWhereMismatchException(replaceWhere: String, badPartitions: String): Throwable = {
    new AnalysisException(
      s"""Data written out does not match replaceWhere '$replaceWhere'.
         |Invalid data would be written to partitions $badPartitions.""".stripMargin)
  }

  def subqueryNotSupportedException(op: String, cond: Expression): Throwable = {
    new AnalysisException(s"Subqueries are not supported in the $op (condition = ${cond.sql}).")
  }

  def notALakeSoulTableException(lakeSoulTableName: String): Throwable = {
    new AnalysisException(s"$lakeSoulTableName is not an LakeSoul table.")
  }

  def notALakeSoulSourceException(command: String, plan: Option[LogicalPlan] = None): Throwable = {
    val planName = if (plan.isDefined) plan.toString else ""
    new AnalysisException(s"$command destination only supports lakesoul sources.\n$planName")
  }


  def updateSetColumnNotFoundException(col: String, colList: Seq[String]): Throwable = {
    new AnalysisException(
      s"SET column ${formatColumn(col)} not found given columns: ${formatColumnList(colList)}.")
  }

  def updateSetConflictException(cols: Seq[String]): Throwable = {
    new AnalysisException(
      s"There is a conflict from these SET columns: ${formatColumnList(cols)}.")
  }

  def updateNonStructTypeFieldNotSupportedException(col: String, s: DataType): Throwable = {
    new AnalysisException(
      s"Updating nested fields is only supported for StructType, but you are trying to update " +
        s"a field of ${formatColumn(col)}, which is of type: $s.")
  }

  def partitionPathParseException(fragment: String): Throwable = {
    new AnalysisException(
      "A range partition path fragment should be the form like `range_part=20200507`. "
        + s"The partition path: $fragment")
  }

  def failCommitDataFile(): Throwable = {
    new MetaException(
      s"""
         |Error: Failed to add data file,
         |this batch uuid may already exists, this is unexpected, please have a check.
         """
    )
  }

  def partitionPathInvolvesNonPartitionColumnException(badColumns: Seq[String], fragment: String): Throwable = {

    new AnalysisException(
      s"Non-partitioning column(s) ${formatColumnList(badColumns)} are specified: $fragment")
  }

  def illegalUsageException(option: String, operation: String): Throwable = {
    throw new IllegalArgumentException(
      s"The usage of $option is not allowed when $operation a LakeSoul table.")
  }

  def createExternalTableWithoutSchemaException(path: Path, tableName: String): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create an external table $tableName
         |from `$path` using LakeSoul, but the schema is not specified.
       """.stripMargin)
  }

  def createManagedTableWithoutSchemaException(tableName: String): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create a managed table $tableName
         |using LakeSoul, but the schema is not specified.
       """.stripMargin)
  }

  def invalidColumnName(name: String): Throwable = {
    new AnalysisException(
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def createTableWithDifferentSchemaException(path: Path,
                                              specifiedSchema: StructType,
                                              existingSchema: StructType,
                                              diffs: Seq[String]): Throwable = {
    new AnalysisException(
      s"""The specified schema does not match the existing schema at $path.
         |
         |== Specified ==
         |${specifiedSchema.treeString}
         |
         |== Existing ==
         |${existingSchema.treeString}
         |
         |== Differences==
         |${diffs.map("\n".r.replaceAllIn(_, "\n  ")).mkString("- ", "\n- ", "")}
         |
         |If your intention is to keep the existing schema, you can omit the
         |schema from the create table command. Otherwise please ensure that
         |the schema matches.
        """.stripMargin)
  }

  def createTableWithDifferentPartitioningException(path: Path,
                                                    specifiedColumns: Seq[String],
                                                    existingColumns: Seq[String]): Throwable = {
    new AnalysisException(
      s"""The specified partitioning does not match the existing partitioning at $path.
         |
         |== Specified ==
         |${specifiedColumns.mkString(", ")}
         |
         |== Existing ==
         |${existingColumns.mkString(", ")}
        """.stripMargin)
  }

  def createTableWithDifferentPropertiesException(path: Path,
                                                  specifiedProperties: Map[String, String],
                                                  existingProperties: Map[String, String]): Throwable = {
    new AnalysisException(
      s"""The specified properties do not match the existing properties at $path.
         |
         |== Specified ==
         |${specifiedProperties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
         |
         |== Existing ==
         |${existingProperties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
        """.stripMargin)
  }

  def schemaNotProvidedException: Throwable = {
    new AnalysisException(
      "Table schema is not provided. Please provide the schema of the table when using " +
        "REPLACE table and an AS SELECT query is not provided.")
  }

  def operationNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: `$operation` is not supported for LakeSoul tables")
  }

  def operationNotSupportedException(operation: String, tableIdentifier: Option[TableIdentifier]): Throwable = {
    tableIdentifier match {
      case None => operationNotSupportedException(operation)
      case Some(identifier) =>
        new AnalysisException(
          s"Operation not allowed: `$operation` is not supported " +
            s"for lakesoul tables: $tableIdentifier")
    }
  }

  def alterTableChangeColumnException(oldColumns: String, newColumns: String): Throwable = {
    new AnalysisException(
      "ALTER TABLE CHANGE COLUMN is not supported for changing column " + oldColumns + " to "
        + newColumns)
  }

  def columnNotInSchemaException(column: String, schema: StructType): Throwable = {
    throw new AnalysisException(
      s"Couldn't find column $column in:\n${schema.treeString}")
  }

  def alterTableReplaceColumnsException(
                                         oldSchema: StructType,
                                         newSchema: StructType,
                                         reason: String): Throwable = {
    new AnalysisException(
      s"""Unsupported ALTER TABLE REPLACE COLUMNS operation. Reason: $reason
         |
         |Failed to change schema from:
         |${formatSchema(oldSchema)}
         |to:
         |${formatSchema(newSchema)}""".stripMargin)
  }

  def failCommitReachMaxAttemptsException(table: String, times: Int): Throwable = {
    new AnalysisException(
      s"""This job commit failed for $times times, there may too many updates for table `$table`.""".stripMargin)
  }


  def schemaChangedException(table: String): Throwable = {
    new AnalysisException(
      s"""Schema has been changed for table `$table`. Please check and retry.""")
  }

  def hashBucketNumNotSetException(): Throwable = {
    new AnalysisException(
      s"""You must set the bucket num use `.option("hashBucketNum","20")` when you defined hash partition columns.""")
  }

  def notEnoughColumnsInInsert(table: String,
                               query: Int,
                               target: Int,
                               nestedField: Option[String] = None): Throwable = {
    val nestedFieldStr = nestedField.map(f => s"not enough nested fields in $f")
      .getOrElse("not enough data columns")
    new AnalysisException(s"Cannot write to '$table', $nestedFieldStr; " +
      s"target table has $target column(s) but the inserted data has " +
      s"$query column(s)")
  }

  def cannotInsertIntoColumn(tableName: String,
                             source: String,
                             target: String,
                             targetType: String): Throwable = {
    new AnalysisException(
      s"Struct column $source cannot be inserted into a $targetType field $target in $tableName.")
  }


  def schemaChangedSinceAnalysis(atAnalysis: StructType, latestSchema: StructType): Throwable = {
    val schemaDiff = SchemaUtils.reportDifferences(atAnalysis, latestSchema)
      .map(_.replace("Specified", "Latest"))
    new AnalysisException(
      s"""The schema of your LakeSoul table has changed in an incompatible way since your DataFrame or
         |LakeSoulTableRel object was created. Please redefine your DataFrame or LakeSoulTableRel object.
         |Changes:\n${schemaDiff.mkString("\n")}
       """.stripMargin)
  }

  def wrongUpsertModeException(mode: String): Throwable = {
    new AnalysisException(
      s"""Option upsertMode must be set as `upsert` or `update`, but there is `$mode`.""")
  }

  def appendNotSupportException: Throwable = {
    new AnalysisException(
      s"""When use hash partition and not first commit, `Append` mode is not supported.""")
  }

  def aggsNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new AnalysisException(s"Aggregate functions are not supported in the $op $condStr.")
  }

  def nonDeterministicNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new AnalysisException(s"Non-deterministic functions are not supported in the $op $condStr.")
  }

  def unknownUpsertModeException(mode: String): Throwable = {
    new AnalysisException(s"Unknown UpsertMode was specified: $mode, please use `upsert` or `update` instead")
  }

  def partitionColumnNotFoundException(column: String, sourceCols: String): Throwable = {
    throw new AnalysisException(
      s"Couldn't find all the partition columns `$column` while upsert, the sourceDF columns are `$sourceCols`.")
  }

  def partitionColumnNotFoundException(condition: Expression, size: Int): Throwable = {
    size match {
      case 0 => throw new AnalysisException(
        s"Couldn't execute compaction because of your condition `${condition.toString()}` touched no files.")
      case _ => throw new AnalysisException(
        s"Couldn't execute compaction because of your condition `${condition.toString()}` refer to $size partitions, " +
          s"but we only allow one partition.")
    }
  }

  def tableNotFoundException(table_name: String, table_id: String): Throwable = {
    throw new AnalysisException(
      s"Table `$table_name` with id=`$table_id` was not found.")
  }

  def partitionNotFoundException(table_name: String, condition: String): Throwable = {
    throw new AnalysisException(
      s"Partition not found by condition `$condition` for table `$table_name`.")
  }

  def tooMuchPartitionException(table_name: String, condition: String, num: Int): Throwable = {
    throw new AnalysisException(
      s"You can only drop one partition once time, there were $num partitions found" +
        s" by condition `$condition` for table `$table_name`.")
  }

  def configureSparkSessionWithExtensionAndCatalog(originalException: Throwable): Throwable = {
    val catalogImplConfig = SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key
    new AnalysisException(
      s"""This LakeSoul operation requires the SparkSession to be configured with the
         |LakeSoulSparkSessionExtension and the LakeSoulCatalog. Please set the necessary
         |configurations when creating the SparkSession as shown below.
         |
         |  SparkSession.builder()
         |    .option("spark.sql.extensions", "${classOf[LakeSoulSparkSessionExtension].getName}")
         |    .option("$catalogImplConfig", "${classOf[LakeSoulCatalog].getName}"
         |    ...
         |    .build()
      """.stripMargin,
      cause = Some(originalException))
  }

  def filePathNotFoundException(filePath: String, dataInfoPath: String): Throwable = {
    throw new AnalysisException(
      s"""
         |File path are not equal while getting MergePartitionedFile,
         |file path: $filePath
         |data info path: $dataInfoPath
      """.stripMargin)
  }

  def columnsNotFoundException(cols: Seq[String]): Throwable = {
    throw new AnalysisException(
      s"""
         |Can't find column ${cols.mkString(",")} in table schema.
      """.stripMargin)
  }

  def hashColumnsIsNullException(): Throwable = {
    throw new AnalysisException(
      s"""
         |Table should define Hash partition column to use upsert.
      """.stripMargin)
  }

  def upsertConditionNotFoundException(): Throwable = {
    throw new AnalysisException(
      s"""
         |Some condition for range partition should be declared to prevent full table scan when upsert.
         |You can also set spark.dmetasoul.lakesoul.full.partitioned.table.scan.enabled to true,
         |or set spark.dmetasoul.lakesoul.deltaFile.enabled to true.
      """.stripMargin)
  }

  def lakeSoulRelationIllegalException(): Throwable = {
    throw new AnalysisException(
      s"""
         |Relation type is illegal, only LakeSoulTableRel should exists here.
      """.stripMargin)
  }

  def useMergeOperatorForNonLakeSoulTableField(fieldName: String): Throwable = {
    new AnalysisException(s"Field `$fieldName` is not in LakeSoulTableRel, you can't perform merge operator on it")
  }

  def illegalMergeOperatorException(cls: Any): Throwable = {
    new AnalysisException(s"${cls.toString} is not a legal merge operator class")
  }

  def multiMergeOperatorException(fieldName: String): Throwable = {
    new AnalysisException(s"Column `$fieldName` has multi merge operators, but only one merge operator can be set.")
  }

  def compactionFailedWithPartMergeException(): Throwable = {
    new MetaException("Compaction with part merging commit failed, another job may had compacted this partition.")
  }

  def unsupportedLogicalPlanWhileRewriteQueryException(plan: String): Throwable = {
    new AnalysisException(
      s"""Found unsupported logical plan while rewrite Query.
          Unsupported plan:
          $plan
       """.stripMargin)
  }
}


/** A helper class in building a helpful error message in case of metadata mismatches. */
class MetadataMismatchErrorBuilder {
  private var bits: Seq[String] = Nil

  private var mentionedOption = false

  def addSchemaMismatch(original: StructType, data: StructType): Unit = {
    bits ++=
      s"""A schema mismatch detected when writing to the table.
         |To enable schema migration, please set:
         |'.option("${LakeSoulOptions.MERGE_SCHEMA_OPTION}", "true")'.
         |
         |Table schema:
         |${LakeSoulErrors.formatSchema(original)}
         |
         |Data schema:
         |${LakeSoulErrors.formatSchema(data)}
         """.stripMargin :: Nil
    mentionedOption = true
  }

  def addPartitioningMismatch(original: Seq[String], provided: Seq[String]): Unit = {
    bits ++=
      s"""Partition columns do not match the partition columns of the table.
         |Given: ${LakeSoulErrors.formatColumnList(provided)}
         |Table: ${LakeSoulErrors.formatColumnList(original)}
         """.stripMargin :: Nil
  }

  def addPartitioningMismatch(original: String, provided: Seq[String]): Unit = {
    addPartitioningMismatch(Seq(original), provided)
  }

  def addOverwriteBit(): Unit = {
    bits ++=
      s"""To overwrite your data schema, please set:
         |'.option("${LakeSoulOptions.OVERWRITE_SCHEMA_OPTION}", "true")'.
         |
         |Note that you can't change partition columns, and the schema can't be overwritten when using
         |'${LakeSoulOptions.REPLACE_WHERE_OPTION}'.
         """.stripMargin :: Nil
    mentionedOption = true
  }

  def finalizeAndThrow(): Unit = {
    if (mentionedOption) {
      bits ++=
        """If Table ACLs are enabled, these options will be ignored. Please use the ALTER TABLE
          |command for changing the schema.
        """.stripMargin :: Nil
    }
    throw new AnalysisException(bits.mkString("\n"))
  }
}
