package org.apache.spark.sql.lakesoul.lineage

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.{Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.lit

object PrintLogicalPlan {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local")
      //.config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      // 使用 SQL 功能还需要增加以下两个配置项
      //.config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
      .config("spark.openlineage.transport.type","http")
      //.config("spark.openlineage.namespace", "my-namespace")
      .config("spark.openlineage.jobName.type","static")
      .config("spark.openlineage.schemaExtraction.enabled", "false")  // 关闭采样
      .config("spark.openlineage.facets.schema.enabled", "true")
      .config("spark.openlineage.transport.url","http://localhost:5000")
      //.config("openlineage.transport.type", "console")   // 打印到控制台
      //.config("spark.sql.defaultCatalog", "lakesoul")
      .appName("spark读写")
      .getOrCreate()
    import spark.implicits._

//    var df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 2, "bread")).toDF("date", "id", "name")
//    val tablePath = "file:///tmp/bucket-name/table/path/is/also/table/name"
//    df.write
//      .mode("overwrite")
//      .format("lakesoul")
//      .option("rangePartitions","date")
//      .option("hashPartitions","id")
//      .option("hashBucketNum","2")
//      .save(tablePath)


//    spark.sql("show create table `default`.s_test_cdc_default_init").show()
 //   spark.sql("insert overwrite s_test_cdc_default_init_3 select rowKinds,user_id,username from s_test_cdc_default_init")

    spark
      .read
      .parquet("/home/cyh/data/lakesoul/sqlserver/flink/data/default/s_dbo_users/part-5jxpKJZkx3vFa8ij_0000.parquet")
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/home/cyh/data/lineage")
    //println(df.queryExecution.logical.numberedTreeString)
    //df.explain(true)

//LakeSoulTable.forPath(tablePath).toDF.select("id").withColumn("new_col",lit(true)).show()

//    val plan = df1.logicalPlan
//    plan match {
//      case DataSourceV2Relation(table, output, catalog, identifier, options) =>
//
//        println(table.schema())
//        val tableName = table.name()
//        println(tableName)
//      case LogicalRelation(relation, output, catalogTable, isStreaming) =>
//        val tableInfo = relation.schema.fieldNames
//        println(tableInfo)
//      case LocalRelation(output, data, isStreaming) =>
//        println(data)
//      case _ => plan
//    }
//
//    df = df.select("id").toDF()
//
//    println("=====")
//    df.explain(true)


    spark.close()
  }
}
