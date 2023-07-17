// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

object CHBenchmark {

  val query_1 = "SELECT ol_number, sum(ol_quantity) AS sum_qty, sum(ol_amount) AS sum_amount, avg(ol_quantity) AS avg_qty, avg(ol_amount) AS avg_amount, count(*) AS count_order FROM `order_line` WHERE ol_delivery_d > TIMESTAMP '2007-01-02 00:00:00.000000' GROUP BY ol_number ORDER BY ol_number"
  val query_2 = "SELECT su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment FROM `item`, `supplier`, `stock`, `nation`, `region`, (SELECT s_i_id AS m_i_id, MIN(s_quantity) AS m_s_quantity FROM `stock`, `supplier`, `nation`, `region` WHERE MOD((s_w_id*s_i_id), 10000)=su_suppkey AND su_nationkey=n_nationkey AND n_regionkey=r_regionkey AND r_name LIKE 'Europ%' GROUP BY s_i_id) m WHERE i_id = s_i_id AND MOD((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND i_data LIKE '%b' AND r_name LIKE 'Europ%' AND i_id=m_i_id AND s_quantity = m_s_quantity ORDER BY n_name, su_name, i_id"
  val query_3 = "SELECT ol_o_id, ol_w_id, ol_d_id, sum(ol_amount) AS revenue, o_entry_d FROM `customer`, `new_order`, `oorder`, `order_line` WHERE c_state LIKE 'A%' AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND no_w_id = o_w_id AND no_d_id = o_d_id AND no_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d > TIMESTAMP '2007-01-02 00:00:00.000000' GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d ORDER BY revenue DESC , o_entry_d"
  val query_4 = "SELECT o_ol_cnt, count(*) AS order_count FROM `oorder` WHERE exists (SELECT * FROM `order_line` WHERE o_id = ol_o_id AND o_w_id = ol_w_id AND o_d_id = ol_d_id AND ol_delivery_d >= o_entry_d) GROUP BY o_ol_cnt ORDER BY o_ol_cnt"
  val query_5 = "SELECT n_name, sum(ol_amount) AS revenue FROM `customer`, `oorder`, `order_line`, `stock`, `supplier`, `nation`, `region` WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id=o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND MOD((s_w_id * s_i_id), 10000) = su_suppkey AND ascii(substring(c_state from  1  for  1)) = su_nationkey AND su_nationkey = n_nationkey AND n_regionkey = r_regionkey AND r_name = 'Europe' AND o_entry_d >= TIMESTAMP '2007-01-02 00:00:00.000000' GROUP BY n_name ORDER BY revenue DESC"
  val query_6 = "SELECT sum(ol_amount) AS revenue FROM `order_line` WHERE ol_delivery_d >= TIMESTAMP '1999-01-01 00:00:00.000000' AND ol_delivery_d < TIMESTAMP '2020-01-01 00:00:00.000000' AND ol_quantity BETWEEN 1 AND 100000"
  val query_7 = "SELECT su_nationkey AS supp_nation, substring(c_state from 1 for 1) AS cust_nation, extract(YEAR FROM o_entry_d) AS l_year, sum(ol_amount) AS revenue FROM `supplier`, `stock`, `order_line`, `oorder`, `customer`, `nation` n1, `nation` n2 WHERE ol_supply_w_id = s_w_id AND ol_i_id = s_i_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND su_nationkey = n1.n_nationkey AND ascii(substring(c_state from  1  for  1)) = n2.n_nationkey AND ((n1.n_name = 'Germany' AND n2.n_name = 'Cambodia') OR (n1.n_name = 'Cambodia' AND n2.n_name = 'Germany')) GROUP BY su_nationkey, substring(c_state from 1 for 1), extract(YEAR FROM o_entry_d) ORDER BY su_nationkey, substring(c_state from 1 for 1), extract(YEAR FROM o_entry_d)"
  val query_8 = "SELECT extract(YEAR FROM o_entry_d) AS l_year, sum(CASE WHEN n2.n_name = 'Germany' THEN ol_amount ELSE 0 END) / sum(ol_amount) AS mkt_share FROM `item`, `supplier`, `stock`, `order_line`, `oorder`, `customer`, `nation` n1, `nation` n2, `region` WHERE i_id = s_i_id AND ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND n1.n_nationkey = ascii(substring(c_state from  1  for  1)) AND n1.n_regionkey = r_regionkey AND ol_i_id < 1000 AND r_name = 'Europe' AND su_nationkey = n2.n_nationkey AND i_data LIKE '%b' AND i_id = ol_i_id GROUP BY extract(YEAR FROM o_entry_d) ORDER BY extract(YEAR FROM o_entry_d)"
  val query_9 = "SELECT n_name, extract(YEAR FROM o_entry_d) AS l_year, sum(ol_amount) AS sum_profit FROM `item`, `stock`, `supplier`, `order_line`, `oorder`, `nation` WHERE ol_i_id = s_i_id AND ol_supply_w_id = s_w_id AND MOD ((s_w_id * s_i_id), 10000) = su_suppkey AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND ol_i_id = i_id AND su_nationkey = n_nationkey AND i_data LIKE '%bb' GROUP BY n_name, extract(YEAR FROM o_entry_d) ORDER BY n_name, extract(YEAR FROM o_entry_d)  DESC"
  val query_10 = "SELECT c_id, c_last, sum(ol_amount) AS revenue, c_city, c_phone, n_name FROM `customer`, `oorder`, `order_line`, `nation` WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d >= TIMESTAMP '2007-01-02 00:00:00.000000' AND o_entry_d <= ol_delivery_d AND n_nationkey = ascii(substring(c_state from  1  for  1)) GROUP BY c_id, c_last, c_city, c_phone, n_name ORDER BY revenue DESC"
  val query_11 = "SELECT s_i_id, sum(s_order_cnt) AS ordercount FROM `stock`, `supplier`, `nation` WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY s_i_id HAVING sum(s_order_cnt) > (SELECT sum(s_order_cnt) * .005 FROM `stock`, `supplier`, `nation` WHERE mod((s_w_id * s_i_id), 10000) = su_suppkey AND su_nationkey = n_nationkey AND n_name = 'Germany') ORDER BY ordercount DESC"
  val query_12 = "SELECT o_ol_cnt, sum(CASE WHEN o_carrier_id = 1 OR o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count, sum(CASE WHEN o_carrier_id <> 1 AND o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count FROM `oorder`, `order_line` WHERE ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id AND o_entry_d <= ol_delivery_d AND ol_delivery_d < TIMESTAMP '2020-01-01 00:00:00.000000' GROUP BY o_ol_cnt ORDER BY o_ol_cnt"
  val query_13 = "SELECT c_count, count(*) AS custdist FROM (SELECT c_id, count(o_id) AS c_count FROM `customer` LEFT OUTER JOIN `oorder` ON (c_w_id = o_w_id AND c_d_id = o_d_id AND c_id = o_c_id AND o_carrier_id > 8) GROUP BY c_id) AS c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC"
  val query_14 = "SELECT (100.00 * sum(CASE WHEN i_data LIKE 'PR%' THEN ol_amount ELSE 0 END) / (1 + sum(ol_amount))) AS promo_revenue FROM `order_line`, `item` WHERE ol_i_id = i_id AND ol_delivery_d >= TIMESTAMP'2007-01-02 00:00:00.000000' AND ol_delivery_d < TIMESTAMP '2020-01-02 00:00:00.000000'"
  val query_16 = "SELECT i_name, substring(i_data from  1 for 3) AS brand, i_price, count(DISTINCT (mod((s_w_id * s_i_id),10000))) AS supplier_cnt FROM `stock`, `item` WHERE i_id = s_i_id AND i_data NOT LIKE 'zz%' AND (mod((s_w_id * s_i_id),10000) NOT IN (SELECT su_suppkey FROM `supplier` WHERE su_comment LIKE '%bad%')) GROUP BY i_name, substring(i_data from  1 for 3), i_price ORDER BY supplier_cnt DESC"
  val query_17 = "SELECT SUM(ol_amount) / 2.0 AS avg_yearly FROM `order_line`, (SELECT i_id, AVG (ol_quantity) AS a FROM `item`, `order_line` WHERE i_data LIKE '%b' AND ol_i_id = i_id GROUP BY i_id) t WHERE ol_i_id = t.i_id AND ol_quantity < t.a"
  val query_18 = "SELECT c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount) AS amount_sum FROM `customer`, `oorder`, `order_line` WHERE c_id = o_c_id AND c_w_id = o_w_id AND c_d_id = o_d_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_o_id = o_id GROUP BY o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt HAVING sum(ol_amount) > 200 ORDER BY amount_sum DESC, o_entry_d"
  val query_19 = "SELECT sum(ol_amount) AS revenue FROM `order_line`, `item` WHERE (ol_i_id = i_id AND i_data LIKE '%a' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 3)) OR (ol_i_id = i_id AND i_data LIKE '%b' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 2, 4)) OR (ol_i_id = i_id AND i_data LIKE '%c' AND ol_quantity >= 1 AND ol_quantity <= 10 AND i_price BETWEEN 1 AND 400000 AND ol_w_id IN (1, 5, 3))"
  val query_20 = "SELECT su_name, su_address FROM `supplier`, `nation` WHERE su_suppkey IN (SELECT mod(s_i_id * s_w_id, 10000) FROM `stock` INNER JOIN `item` ON i_id = s_i_id INNER JOIN `order_line` ON ol_i_id = s_i_id WHERE ol_delivery_d > TIMESTAMP '2010-05-23 12:00:00' AND i_data LIKE 'co%' GROUP BY s_i_id, s_w_id, s_quantity HAVING 2*s_quantity > sum(ol_quantity)) AND su_nationkey = n_nationkey AND n_name = 'Germany' ORDER BY su_name"
  val query_21 = "SELECT su_name, count(*) AS numwait FROM `supplier`, `order_line` l1, `oorder`, `stock`, `nation` WHERE ol_o_id = o_id AND ol_w_id = o_w_id AND ol_d_id = o_d_id AND ol_w_id = s_w_id AND ol_i_id = s_i_id AND mod((s_w_id * s_i_id),10000) = su_suppkey AND l1.ol_delivery_d > o_entry_d AND NOT EXISTS (SELECT * FROM `order_line` l2 WHERE l2.ol_o_id = l1.ol_o_id AND l2.ol_w_id = l1.ol_w_id AND l2.ol_d_id = l1.ol_d_id AND l2.ol_delivery_d > l1.ol_delivery_d) AND su_nationkey = n_nationkey AND n_name = 'Germany' GROUP BY su_name ORDER BY numwait DESC, su_name"
  val query_22 = "SELECT substring(c_state,1,1) AS country, count(*) AS numcust, sum(c_balance) AS totacctbal FROM `customer` WHERE substring(c_phone from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7') AND c_balance > (SELECT avg(c_balance) FROM `customer` WHERE c_balance > 0.00 AND substring(c_phone from 1 for 1) IN ('1', '2', '3', '4', '5', '6', '7')) AND NOT EXISTS (SELECT * FROM `oorder` WHERE o_c_id = c_id AND o_w_id = c_w_id AND o_d_id = c_d_id) GROUP BY substring(c_state, 1, 1) ORDER BY substring(c_state,1,1)"

  val queryMap: Map[String, String] = Map("query_1" -> query_1, "query_2" -> query_2, "query_3" -> query_3, "query_4" -> query_4, "query_5" -> query_5, "query_6" -> query_6,
    "query_7" -> query_7, "query_8" -> query_8, "query_9" -> query_9, "query_10" -> query_10, "query_11" -> query_11, "query_12" -> query_12,
    "query_13" -> query_13, "query_14" -> query_14, "query_16" -> query_16, "query_17" -> query_17, "query_18" -> query_18, "query_19" -> query_19,
    "query_20" -> query_20, "query_21" -> query_21, "query_22" -> query_22)

  var hostname = "mysql"
  var dbName = "test_cdc"
  var mysqlUserName = "root"
  var mysqlPassword = "root"
  var mysqlPort = 3306
  var serverTimeZone = "UTC"

  var url: String = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone

  val printLine = " ******** "
  val splitLine = " --------------------------------------------------------------- "

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CH_BENCHMARK TEST")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "16")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if (args.length >= 7) {
      hostname = args(1)
      dbName = args(2)
      mysqlUserName = args(3)
      mysqlPassword = args(4)
      mysqlPort = args(5).toInt
      serverTimeZone = args(6)
      url = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone
    }

    spark.sql("use " + dbName)

    //    queryTest(spark, query_18, "query_18")

    println(splitLine)
    queryMap.foreach(k => queryTest(spark, k._2, k._1))
    println(splitLine)

    if (args.length >= 1 && args(0) == "--verifyQuery") {
      queryMap.foreach(
        k => k._1 match {
          case "query_1" => verifyQuery1(spark, k._2, k._1)
          case "query_8" => verifyQuery8(spark, k._2, k._1)
          case "query_11" => verifyQuery11(spark, k._2, k._1)
          case "query_12" => verifyQuery12(spark, k._2, k._1)
          case "query_14" => verifyQuery14(spark, k._2, k._1)
          case _ => verifyQuery(spark, k._2, k._1)
        }
      )
    }
  }

  def queryTest(spark: SparkSession, query: String, queryNum: String): Unit = {
    val startTime = System.currentTimeMillis()
    spark.sql(query.stripMargin).write.format("noop").mode("overwrite").save()
    println(printLine + queryNum + ":" + (System.currentTimeMillis() - startTime) + printLine)
  }

  def verifyQuery(spark: SparkSession, query: String, queryNum: String): Unit = {
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16").load()
    val lakesoulDF = spark.sql(query)
    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }

  def verifyQuery1(spark: SparkSession, query: String, queryNum: String): Unit = {
    import org.apache.spark.sql.functions.format_number
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16")
      .load()
      .withColumn("avg_qty", format_number(col("avg_qty"), 1))
      .withColumn("sum_qty", format_number(col("sum_qty"), 0))
    val lakesoulDF = spark.sql(query)
      .withColumn("avg_qty", format_number(col("avg_qty"), 1))
      .withColumn("sum_qty", format_number(col("sum_qty"), 0))

    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }

  def verifyQuery8(spark: SparkSession, query: String, queryNum: String): Unit = {
    import org.apache.spark.sql.functions.format_number
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16")
      .load()
      .withColumn("mkt_share", format_number(col("mkt_share"), 6))

    val lakesoulDF = spark.sql(query)
      .withColumn("mkt_share", format_number(col("mkt_share"), 6))

    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }

  def verifyQuery11(spark: SparkSession, query: String, queryNum: String): Unit = {
    import org.apache.spark.sql.functions.format_number
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16")
      .load()
      .withColumn("ordercount", format_number(col("ordercount"), 0))

    val lakesoulDF = spark.sql(query)
      .withColumn("ordercount", format_number(col("ordercount"), 0))

    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }

  def verifyQuery12(spark: SparkSession, query: String, queryNum: String): Unit = {
    import org.apache.spark.sql.functions.format_number
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16")
      .load()
      .withColumn("high_line_count", format_number(col("high_line_count"), 0))
      .withColumn("low_line_count", format_number(col("low_line_count"), 0))

    val lakesoulDF = spark.sql(query)
      .withColumn("high_line_count", format_number(col("high_line_count"), 0))
      .withColumn("low_line_count", format_number(col("low_line_count"), 0))

    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }

  def verifyQuery14(spark: SparkSession, query: String, queryNum: String): Unit = {
    import org.apache.spark.sql.functions.format_number
    val jdbcDF = spark.read.format("jdbc").option("driver", "com.mysql.jdbc.Driver").option("url", url).option("dbtable", "(" + query.replace("LIKE", "LIKE binary") + ") as t").option("user", mysqlUserName).option("password", mysqlPassword).option("numPartitions", "16")
      .load()
      .withColumn("promo_revenue", format_number(col("promo_revenue"), 8))

    val lakesoulDF = spark.sql(query)
      .withColumn("promo_revenue", format_number(col("promo_revenue"), 8))

    println(printLine + queryNum + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }
}
