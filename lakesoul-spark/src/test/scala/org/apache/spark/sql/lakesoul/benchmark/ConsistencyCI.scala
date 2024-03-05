package org.apache.spark.sql.lakesoul.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.types._

object ConsistencyCI {


  val tpchTable = Seq(
    ("customer",
      StructType(Array(
        StructField("c_custkey", LongType, nullable = false),
        StructField("c_name", StringType, nullable = false),
        StructField("c_address", StringType, nullable = false),
        StructField("c_nationkey", LongType, nullable = false),
        StructField("c_phone", StringType, nullable = false),
        StructField("c_acctbal", DecimalType(15, 2), nullable = false),
        StructField("c_mktsegment", StringType, nullable = false),
        StructField("c_comment", StringType, nullable = false),
      )),
      "c_custkey, c_name", Some("c_nationkey")),
    ("part",
      StructType(Array(
        StructField("p_partkey", LongType, nullable = false),
        StructField("p_name", StringType, nullable = false),
        StructField("p_mfgr", StringType, nullable = false),
        StructField("p_brand", StringType, nullable = false),
        StructField("p_type", StringType, nullable = false),
        StructField("p_size", IntegerType, nullable = false),
        StructField("p_container", StringType, nullable = false),
        StructField("p_retailprice", DecimalType(15, 2), nullable = false),
        StructField("p_comment", StringType, nullable = false),
      )),
      "p_partkey, p_name", Option.empty),
    ("supplier",
      StructType(Array(
        StructField("s_suppkey", LongType, nullable = false),
        StructField("s_name", StringType, nullable = false),
        StructField("s_address", StringType, nullable = false),
        StructField("s_nationkey", LongType, nullable = false),
        StructField("s_phone", StringType, nullable = false),
        StructField("s_acctbal", DecimalType(15, 2), nullable = false),
        StructField("s_comment", StringType, nullable = false),
      )),
      "s_suppkey, s_name", Some("s_nationkey")),
    ("partsupp",
      StructType(Array(
        StructField("ps_partkey", LongType, nullable = false),
        StructField("ps_suppkey", LongType, nullable = false),
        StructField("ps_availqty", IntegerType, nullable = false),
        StructField("ps_supplycost", DecimalType(15, 2), nullable = false),
        StructField("ps_comment", StringType, nullable = false),
      )),
      "ps_partkey, ps_suppkey", Option.empty),
    ("orders",
      StructType(Array(
        StructField("o_orderkey", LongType, nullable = false),
        StructField("o_custkey", LongType, nullable = false),
        StructField("o_orderstatus", StringType, nullable = false),
        StructField("o_totalprice", DecimalType(15, 2), nullable = false),
        StructField("o_orderdate", DateType, nullable = false),
        StructField("o_orderpriority", StringType, nullable = false),
        StructField("o_clerk", StringType, nullable = false),
        StructField("o_shippriority", IntegerType, nullable = false),
        StructField("o_comment", StringType, nullable = false),
      )),
      "o_orderkey, o_custkey", Some("o_orderpriority")),

    ("nation",
      StructType(Array(
        StructField("n_nationkey", LongType, nullable = false),
        StructField("n_name", StringType, nullable = false),
        StructField("n_regionkey", LongType, nullable = false),
        StructField("n_comment", StringType, nullable = false),
      )),
      "n_nationkey, n_name", Some("n_regionkey")),
    ("region",
      StructType(Array(
        StructField("r_regionkey", LongType, nullable = false),
        StructField("r_name", StringType, nullable = false),
        StructField("r_comment", StringType, nullable = false),
      )),
      "r_regionkey, r_name", Option.empty),
    ("lineitem",
      StructType(Array(
        StructField("l_orderkey", LongType, nullable = false),
        StructField("l_partkey", LongType, nullable = false),
        StructField("l_suppkey", LongType, nullable = false),
        StructField("l_linenumber", IntegerType, nullable = false),
        StructField("l_quantity", DecimalType(15, 2), nullable = false),
        StructField("l_extendedprice", DecimalType(15, 2), nullable = false),
        StructField("l_discount", DecimalType(15, 2), nullable = false),
        StructField("l_tax", DecimalType(15, 2), nullable = false),
        StructField("l_returnflag", StringType, nullable = false),
        StructField("l_linestatus", StringType, nullable = false),
        StructField("l_shipdate", DateType, nullable = false),
        StructField("l_commitdate", DateType, nullable = false),
        StructField("l_receiptdate", DateType, nullable = false),
        StructField("l_shipinstruct", StringType, nullable = false),
        StructField("l_shipmode", StringType, nullable = false),
        StructField("l_comment", StringType, nullable = false),
      )),
      "l_orderkey, l_partkey", Option.empty),
  )

  def load_data(spark: SparkSession): Unit = {

    val tpchPath = System.getenv("TPCH_DATA")
    val lakeSoulPath = "/tmp/lakesoul/tpch"
    tpchTable.foreach(tup => {
      val (name, schema, hashPartitions, rangePartitions) = tup
      val df = spark.read.option("delimiter", "|")
        .schema(schema)
        .csv(s"$tpchPath/$name.tbl")
      //      df.show
      rangePartitions match {
        case Some(value) =>
          df.write.format("lakesoul")
            .option("shortTableName", name)
            .option("hashPartitions", hashPartitions)
            .option("rangePartitions", value)
            .option("hashBucketNum", 5)
            .mode("Overwrite")
            .save(s"$lakeSoulPath/$name")
        case None =>
          df.write.format("lakesoul")
            .option("shortTableName", name)
            .option("hashPartitions", hashPartitions)
            .option("hashBucketNum", 5)
            .mode("Overwrite")
            .save(s"$lakeSoulPath/$name")
      }

    })

  }

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("Consistency CI")
      .master("local[4]")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "16")
      .config("spark.sql.parquet.binaryAsString", "true")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("show tables")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS tpch")
    spark.sql("USE tpch")

    load_data(spark)

    tpchTable.foreach(tup => {
      val sparkDF = spark.sql(s"select * from ${tup._1}")
      val rustDF = spark.sql(s"select * from default.${tup._1}")
      println(s"${tup._1} sparkDF: ")
      sparkDF.show
      println(s"${tup._1} rustDF: ")
      rustDF.show
      val diff1 = sparkDF.rdd.subtract(rustDF.rdd)
      val diff2 = rustDF.rdd.subtract(sparkDF.rdd)
      val result = diff1.count() == 0 && diff2.count() == 0
      if (!result) {
        println("sparkDF: ")
        println(sparkDF.collectAsList())
        println("rustDF: ")
        println(rustDF.collectAsList())
        System.exit(1)
      }
    })

  }
}
