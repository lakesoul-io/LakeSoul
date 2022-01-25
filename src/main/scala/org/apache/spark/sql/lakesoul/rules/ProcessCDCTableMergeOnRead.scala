package org.apache.spark.sql.lakesoul.rules
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.{LakeSoulTableProperties, LakeSoulTableRelationV2}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
case class ProcessCDCTableMergeOnRead (sqlConf: SQLConf) extends Rule[LogicalPlan]{
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown   {
    case p: LogicalPlan if p.children.exists(_.isInstanceOf[DataSourceV2Relation]) && !p.isInstanceOf[Filter] =>
      p.children.toSeq.find(_.isInstanceOf[DataSourceV2Relation]).get match  {
      case dsv2@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _)=>{
        val value=getLakeSoulTableCDCColumn(table)
        if(value.nonEmpty){
          p.withNewChildren(Filter(Column(expr(s" ${value.get}!= 'delete'").expr).expr,dsv2)::Nil)
        }
        else {
          p
        }
      }

    }
   }
  private def lakeSoulTableHasHashPartition(table: LakeSoulTableV2): Boolean = {
    table.snapshotManagement.snapshot.getTableInfo.hash_column.nonEmpty
  }
  private def lakeSoulTableCDCColumn(table: LakeSoulTableV2): Boolean = {
    table.snapshotManagement.snapshot.getTableInfo.configuration.contains(LakeSoulTableProperties.lakeSoulCDCChangePropKey)
  }
  private def getLakeSoulTableCDCColumn(table: LakeSoulTableV2): Option[String] = {
    table.snapshotManagement.snapshot.getTableInfo.configuration.get(LakeSoulTableProperties.lakeSoulCDCChangePropKey)
  }

}
