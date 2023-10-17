// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_HASH_PARTITION_SPLITTER, LAKESOUL_RANGE_PARTITION_SPLITTER}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.TransactionCommit
import org.apache.spark.sql.lakesoul.exception.{LakeSoulErrors, MetadataMismatchErrorBuilder}
import org.apache.spark.sql.lakesoul.utils.{PartitionUtils, SparkUtil, TableInfo}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.arrow.ArrowUtils


/**
  * A trait that writers into LakeSoulTableRel can extend to update the schema of the table.
  */
trait ImplicitMetadataOperation extends Logging {

  protected val canMergeSchema: Boolean
  protected val canOverwriteSchema: Boolean
  protected val rangePartitions: String
  protected val hashPartitions: String
  protected val hashBucketNum: Int
  protected val shortTableName: Option[String]

  private def transPartitionColumns(partitionColumns: String): Seq[String] = {
    if (partitionColumns.equalsIgnoreCase("")) {
      Seq.empty[String]
    } else {
      partitionColumns.split(LAKESOUL_RANGE_PARTITION_SPLITTER).toSeq
    }
  }

  private def normalizePartitionColumns(spark: SparkSession,
                                        partitionCols: Seq[String],
                                        schema: StructType): Seq[String] = {
    partitionCols.map { columnName =>
      val colMatches = schema.filter(s => SchemaUtils.COL_RESOLVER(s.name, columnName))
      if (colMatches.length > 1) {
        throw LakeSoulErrors.ambiguousPartitionColumnException(columnName, colMatches)
      } else if (colMatches.isEmpty) {
        throw LakeSoulErrors.partitionColumnNotFoundException(columnName, schema.toAttributes)
      }
      colMatches.head.name
    }
  }

  protected final def updateMetadata(tc: TransactionCommit,
                                     data: Dataset[_],
                                     configuration: Map[String, String],
                                     isOverwriteMode: Boolean): Unit = {
    updateMetadata(
      data.sparkSession,
      tc,
      data.schema,
      configuration,
      isOverwriteMode,
      Some(data)
    )
  }

  protected final def updateMetadata(spark: SparkSession,
                                     tc: TransactionCommit,
                                     schema: StructType,
                                     configuration: Map[String, String],
                                     isOverwriteMode: Boolean,
                                     data: Option[Dataset[_]] = None): Unit = {
    val table_info = tc.tableInfo

    /**
      * If it is the first commit (e.g. create table), the parameters in OPTION are used;
      * otherwise, the parameters in META are used.
      */
    val (realRangeColumns, realHashColumns, realHashBucketNum) =
      if (tc.isFirstCommit) {
        (transPartitionColumns(rangePartitions), transPartitionColumns(hashPartitions), hashBucketNum)
      } else {
        //If the partition parameters are set and the table already exists, the settings must be the same as the table
        if (rangePartitions.nonEmpty && !table_info.range_column.equalsIgnoreCase(rangePartitions)) {
          throw LakeSoulErrors.partitionColumnConflictException(table_info.range_column, rangePartitions, "Range")
        }
        if (hashPartitions.nonEmpty && !table_info.hash_column.equalsIgnoreCase(hashPartitions)) {
          throw LakeSoulErrors.partitionColumnConflictException(table_info.hash_column, hashPartitions, "Hash")
        }
        if (hashBucketNum != -1 && table_info.bucket_num != hashBucketNum) {
          throw LakeSoulErrors.hashBucketNumConflictException(table_info.bucket_num, hashBucketNum)
        }
        (transPartitionColumns(table_info.range_column), transPartitionColumns(table_info.hash_column), table_info.bucket_num)
      }

    if (shortTableName.isDefined) {
      tc.setShortTableName(shortTableName.get)
    }

    val normalizedRangePartitionCols =
      normalizePartitionColumns(spark, realRangeColumns, schema)
    val normalizedHashPartitionCols =
      normalizePartitionColumns(spark, realHashColumns, schema)

    val dataSchema = StructType(schema.map {
      case StructField(name, dataType, nullable, metadata) =>
        if (normalizedRangePartitionCols.contains(name) || normalizedHashPartitionCols.contains(name)) {
          StructField(name, dataType, nullable = false, metadata)
        } else {
          StructField(name, dataType.asNullable, nullable = true, metadata)
        }
    })


    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      SchemaUtils.mergeSchemas(table_info.schema, dataSchema)
    }

    // Merged schema will contain additional columns at the end
    def isNewSchema: Boolean = table_info.schema != mergedSchema

    PartitionUtils.validatePartitionColumn(
      mergedSchema,
      normalizedRangePartitionCols ++ normalizedHashPartitionCols,
      normalizedRangePartitionCols,
      // LakeSoul is case insensitive regarding internal column naming
      caseSensitive = false)

    if (tc.isFirstCommit) {
      if (dataSchema.isEmpty) {
        throw LakeSoulErrors.emptyDataException
      }

      if (normalizedHashPartitionCols.nonEmpty) {
        if (realHashBucketNum == -1) {
          throw LakeSoulErrors.hashBucketNumNotSetException()
        }
      }

      // If this is the first write, configure the metadata of the table.
      //todo: setting
      tc.updateTableInfo(
        TableInfo(
          namespace = table_info.namespace,
          table_path_s = Option(SparkUtil.makeQualifiedTablePath(new Path(table_info.table_path_s.get)).toString),
          table_id = table_info.table_id,
          table_schema = ArrowUtils.toArrowSchema(dataSchema).toJson,
          range_column = normalizedRangePartitionCols.mkString(LAKESOUL_RANGE_PARTITION_SPLITTER),
          hash_column = normalizedHashPartitionCols.mkString(LAKESOUL_HASH_PARTITION_SPLITTER),
          bucket_num = realHashBucketNum,
          configuration = configuration,
          short_table_name = table_info.short_table_name))
    }
    else if (isOverwriteMode && canOverwriteSchema && isNewSchema) {
      val newTableInfo = tc.tableInfo.copy(
        table_schema = ArrowUtils.toArrowSchema(dataSchema).toJson
      )

      tc.updateTableInfo(newTableInfo)
    } else if (isNewSchema && canMergeSchema) {
      logInfo(s"New merged schema: ${mergedSchema.treeString}")

      tc.updateTableInfo(tc.tableInfo.copy(table_schema = ArrowUtils.toArrowSchema(mergedSchema).toJson))
    } else if (isNewSchema) {
      val errorBuilder = new MetadataMismatchErrorBuilder
      if (isNewSchema) {
        errorBuilder.addSchemaMismatch(tc.tableInfo.schema, dataSchema)
      }
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      }
      errorBuilder.finalizeAndThrow()
    }
  }
}
