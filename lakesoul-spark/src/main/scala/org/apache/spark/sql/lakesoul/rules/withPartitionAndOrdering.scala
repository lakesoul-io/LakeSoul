package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class withPartitionAndOrdering(partition: Partitioning,
                                    ordering: Seq[SortOrder],
                                    child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = child.execute()

  override def outputPartitioning: Partitioning = partition

  override def outputOrdering: Seq[SortOrder] = ordering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def supportsColumnar: Boolean = child.supportsColumnar

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (supportsColumnar) {
      child.executeColumnar()
    } else {
      super.executeColumnar()
    }
  }
}
