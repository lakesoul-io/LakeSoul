package org.apache.spark.sql.lakesoul.rules

import io.glutenproject.execution.RowToVeloxColumnarExec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan}

case class PostInjectColumnar(session: SparkSession) extends ColumnarRule {

  private def transform(plan: SparkPlan): SparkPlan = plan match {
    case RowToVeloxColumnarExec(ColumnarToRowExec(child)) =>
      child
    case p =>
      p.withNewChildren(p.children.map(transform))
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
      transform(plan)
  }
}
