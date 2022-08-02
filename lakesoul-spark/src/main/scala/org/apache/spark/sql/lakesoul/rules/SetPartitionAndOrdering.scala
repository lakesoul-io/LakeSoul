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

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, Attribute, Expression, NamedExpression, PredicateHelper, SortOrder}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.v2.merge.{MultiPartitionMergeBucketScan, OnePartitionMergeBucketScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.BucketParquetScan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{SparkSession, Strategy}


case class SetPartitionAndOrdering(session: SparkSession)
  extends Strategy with PredicateHelper {


  private def withProjectAndFilter(project: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   scan: LeafExecNode,
                                   needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

    case PhysicalOperation(project, filters,
    relation@DataSourceV2ScanRelation(
    DataSourceV2Relation(tbl: LakeSoulTableV2, _, _, _, _),
    bucketScan: BucketParquetScan,
    output)) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val hashKeys = bucketScan.tableInfo.hash_partition_columns.flatMap(key => output.find(_.name == key))
      val bucketNum = bucketScan.tableInfo.bucket_num
      val outputPartitioning = HashPartitioning(hashKeys, bucketNum)
      val outputOrdering = hashKeys.map(key => SortOrder(key, Ascending))


      val batchExec = BatchScanExec(relation.output, relation.scan)
      val child = withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar)

      if (hashKeys.forall(key => child.output.map(_.name).contains(key.name))) {
        withPartitionAndOrdering(outputPartitioning, outputOrdering, child) :: Nil
      } else {
        child :: Nil
      }

    case PhysicalOperation(project, filters,
    relation@DataSourceV2ScanRelation(
    DataSourceV2Relation(tbl: LakeSoulTableV2, _, _, _, _),
    mergeScan@OnePartitionMergeBucketScan(_, _, _, _, _, _, _, options: CaseInsensitiveStringMap, _, _, _),
    output)) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val tableInfo = mergeScan.tableInfo
      val hashKeys = tableInfo.hash_partition_columns.flatMap(key => output.find(_.name == key))
      val bucketNum = tableInfo.bucket_num
      val outputPartitioning = HashPartitioning(hashKeys, bucketNum)

      val isCompaction = options.getOrDefault("isCompaction", "false").equals("true")

      val outputOrdering = if (isCompaction) {
        val rangeKeys = tableInfo.range_partition_columns.flatMap(key => output.find(_.name == key))
        (rangeKeys ++
          Seq(HashPartitioning(hashKeys, tableInfo.bucket_num).partitionIdExpression) ++
          hashKeys).map(key => SortOrder(key, Ascending))

      } else {
        hashKeys.map(key => SortOrder(key, Ascending))
      }


      val batchExec = BatchScanExec(relation.output, relation.scan)

      val child = if (isCompaction) {
        batchExec
      } else {
        withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar)
      }

      if (hashKeys.forall(key => child.output.map(_.name).contains(key.name))) {
        withPartitionAndOrdering(outputPartitioning, outputOrdering, child) :: Nil
      } else {
        child :: Nil
      }

    case PhysicalOperation(project, filters,
    relation@DataSourceV2ScanRelation(
    DataSourceV2Relation(tbl: LakeSoulTableV2, _, _, _, _),
    mergeScan@MultiPartitionMergeBucketScan(_, _, _, _, _, _, _, options: CaseInsensitiveStringMap, _, _, _),
    output)) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val tableInfo = mergeScan.tableInfo
      val hashKeys = tableInfo.hash_partition_columns.flatMap(key => output.find(_.name == key))
      val bucketNum = tableInfo.bucket_num
      val outputPartitioning = HashPartitioning(hashKeys, bucketNum)

      val batchExec = BatchScanExec(relation.output, relation.scan)
      val child = withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar)

      if (hashKeys.forall(key => child.output.map(_.name).contains(key.name))) {
        withPartition(outputPartitioning, child) :: Nil
      } else {
        child :: Nil
      }

    case _ => Nil
  }

}

case class withPartition(partition: Partitioning,
                         child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = child.execute()

  override def outputPartitioning: Partitioning = partition

}

case class withPartitionAndOrdering(partition: Partitioning,
                                    ordering: Seq[SortOrder],
                                    child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = child.execute()

  override def outputPartitioning: Partitioning = partition

  override def outputOrdering: Seq[SortOrder] = ordering

}
