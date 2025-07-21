// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasource.parquet

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.parquet.filter2.predicate.FilterApi._
import org.apache.parquet.filter2.predicate.Operators.{Eq, Gt, GtEq, Lt, LtEq, NotEq, Column => _}
import org.apache.parquet.filter2.predicate.{FilterPredicate, Operators}
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.parquet.{NumRowGroupsAcc, ParquetFilters, ParquetTest, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.{NativeParquetScan, ParquetScan}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_ENABLE
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.AccumulatorContext
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfter
import org.scalatestplus.junit.JUnitRunner

import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime, ZoneId}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

@RunWith(classOf[JUnitRunner])
class ParquetNativeFilterSuite
  extends ParquetFilterSuite
    with LakeSoulTestUtils
    with BeforeAndAfter {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")

  before {
    LakeSoulCatalog.cleanMeta()
  }

  override def withNestedParquetDataFrame(inputDF: DataFrame)
                                         (runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    withNestedDataFrame(inputDF).foreach { case (newDF, colName, resultFun) =>
      withTempDir { file =>
        newDF.write.format("lakesoul").save(file.getCanonicalPath)
        loadLakeSoulTable(file.getCanonicalPath) { df => runTest(df, colName, resultFun) }
      }
    }
  }

  protected def loadLakeSoulTable(path: String, testVectorized: Boolean = true)
                                 (f: DataFrame => Unit) = {
    f(spark.read.format("lakesoul").load(path.toString).toDF())
  }


  override def checkFilterPredicate(
                                     df: DataFrame,
                                     predicate: Predicate,
                                     filterClass: Class[_ <: FilterPredicate],
                                     checker: (DataFrame, Seq[Row]) => Unit,
                                     expected: Seq[Row]): Unit = {
    val output = predicate.collect { case a: Attribute => a }.distinct

    withSQLConf(
      SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
      SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> "true",
      NATIVE_IO_ENABLE.key -> NativeIOBase.isNativeIOLibExist.toString,
      // Disable adding filters from constraints because it adds, for instance,
      // is-not-null to pushed filters, which makes it hard to test if the pushed
      // filter is expected or not (this had to be fixed with SPARK-13495).
      SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> InferFiltersFromConstraints.ruleName,
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      val query = df
        .select(output.map(e => Column(e)): _*)
        .where(Column(predicate))

      query.queryExecution.optimizedPlan.collectFirst {
        case PhysicalOperation(_, filters,
        DataSourceV2ScanRelation(_, scan: ParquetScan, _, _, _)) =>
          assert(filters.nonEmpty, "No filter is analyzed from the given query")
          val sourceFilters = filters.flatMap(DataSourceStrategy.translateFilter(_, true)).toArray
          val pushedFilters = scan.pushedFilters
          assert(pushedFilters.nonEmpty, "No filter is pushed down")
          val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
          val parquetFilters = createParquetFilters(schema)
          // In this test suite, all the simple predicates are convertible here.
          assert(parquetFilters.convertibleFilters(sourceFilters) === pushedFilters)
          val pushedParquetFilters = pushedFilters.map { pred =>
            val maybeFilter = parquetFilters.createFilter(pred)
            assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
            maybeFilter.get
          }
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(pushedParquetFilters.exists(_.getClass === filterClass),
            s"${pushedParquetFilters.map(_.getClass).toList} did not contain ${filterClass}.")

          checker(stripSparkFilter(query), expected)
        case PhysicalOperation(_, filters,
        DataSourceV2ScanRelation(_, scan: NativeParquetScan, _, _, _)) =>
          println("match case NativeParquetScan")
          //          assert(filters.nonEmpty, "No filter is analyzed from the given query")
          val sourceFilters = filters.flatMap(DataSourceStrategy.translateFilter(_, true)).toArray
          val pushedFilters = scan.pushedFilters
          assert(pushedFilters.nonEmpty, "No filter is pushed down")
          val schema = new SparkToParquetSchemaConverter(conf).convert(df.schema)
          val parquetFilters = createParquetFilters(schema)
          // In this test suite, all the simple predicates are convertible here.
          //          assert(parquetFilters.convertibleFilters(sourceFilters) === pushedFilters)
          val pushedParquetFilters = pushedFilters.map { pred =>
            val maybeFilter = parquetFilters.createFilter(pred)
            println("pred:" + pred.toString + ", maybeFilter:" + maybeFilter.toString)
            assert(maybeFilter.isDefined, s"Couldn't generate filter predicate for $pred")
            maybeFilter.get
          }
          // Doesn't bother checking type parameters here (e.g. `Eq[Integer]`)
          assert(pushedParquetFilters.exists(_.getClass === filterClass),
            s"${pushedParquetFilters.map(_.getClass).toList} did not contain ${filterClass}.")

          checker(stripSparkFilter(query), expected)
        case _ =>
          throw new AnalysisException("Can not match ParquetTable in the query.")
      }
    }
  }
}

/**
  * A test suite that tests Parquet filter2 API based filter pushdown optimization.
  *
  * NOTE:
  *
  * 1. `!(a cmp b)` is always transformed to its negated form `a cmp' b` by the
  * `BooleanSimplification` optimization rule whenever possible. As a result, predicate `!(a < 1)`
  * results in a `GtEq` filter predicate rather than a `Not`.
  *
  * 2. `Tuple1(Option(x))` is used together with `AnyVal` types like `Int` to ensure the inferred
  * data type is nullable.
  *
  * NOTE:
  *
  * This file intendedly enables record-level filtering explicitly. If new test cases are
  * dependent on this configuration, don't forget you better explicitly set this configuration
  * within the test.
  */
abstract class ParquetFilterSuite extends QueryTest with ParquetTest with SharedSparkSession {

  protected def createParquetFilters(
                                      schema: MessageType,
                                      caseSensitive: Option[Boolean] = None,
                                      datetimeRebaseSpec: RebaseSpec = RebaseSpec(LegacyBehaviorPolicy.CORRECTED)): ParquetFilters =
    new ParquetFilters(schema, conf.parquetFilterPushDownDate, conf.parquetFilterPushDownTimestamp,
      conf.parquetFilterPushDownDecimal, conf.parquetFilterPushDownStringPredicate,
      conf.parquetFilterPushDownInFilterThreshold,
      caseSensitive.getOrElse(conf.caseSensitiveAnalysis),
      datetimeRebaseSpec
    )

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Note that there are many tests here that require record-level filtering set to be true.
    spark.conf.set(SQLConf.PARQUET_RECORD_FILTER_ENABLED.key, "true")
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterEach(): Unit = {
    try {
      spark.conf.unset(SQLConf.PARQUET_RECORD_FILTER_ENABLED.key)
    } finally {
      super.afterEach()
    }
  }

  def checkFilterPredicate(
                            df: DataFrame,
                            predicate: Predicate,
                            filterClass: Class[_ <: FilterPredicate],
                            checker: (DataFrame, Seq[Row]) => Unit,
                            expected: Seq[Row]): Unit

  private def checkFilterPredicate
  (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: Seq[Row])
  (implicit df: DataFrame): Unit = {
    checkFilterPredicate(df, predicate, filterClass, checkAnswer(_, _: Seq[Row]), expected)
  }

  private def checkFilterPredicate[T]
  (predicate: Predicate, filterClass: Class[_ <: FilterPredicate], expected: T)
  (implicit df: DataFrame): Unit = {
    checkFilterPredicate(predicate, filterClass, Seq(Row(expected)))(df)
  }

  /**
    * Takes a sequence of products `data` to generate multi-level nested
    * dataframes as new test data. It tests both non-nested and nested dataframes
    * which are written and read back with Parquet datasource.
    *
    * This is different from [[ParquetTest.withParquetDataFrame]] which does not
    * test nested cases.
    */
  private def withNestedParquetDataFrame[T <: Product : ClassTag : TypeTag](data: Seq[T])
                                                                           (runTest: (DataFrame, String, Any => Any) => Unit): Unit =
    withNestedParquetDataFrame(spark.createDataFrame(data))(runTest)

  protected def withNestedParquetDataFrame(inputDF: DataFrame)
                                          (runTest: (DataFrame, String, Any => Any) => Unit): Unit = {
    withNestedDataFrame(inputDF).foreach { case (newDF, colName, resultFun) =>
      withTempPath { file =>
        newDF.write.format(dataSourceName).save(file.getCanonicalPath)
        readParquetFile(file.getCanonicalPath) { df => runTest(df, colName, resultFun) }
      }
    }
  }

  /**
    * Takes single level `inputDF` dataframe to generate multi-level nested
    * dataframes as new test data. It tests both non-nested and nested dataframes
    * which are written and read back with specified datasource.
    */
  override protected def withNestedDataFrame(inputDF: DataFrame): Seq[(DataFrame, String, Any => Any)] = {
    assert(inputDF.schema.fields.length == 1)
    assert(!inputDF.schema.fields.head.dataType.isInstanceOf[StructType])
    val df = inputDF.toDF("temp")
    Seq(
      (
        df.withColumnRenamed("temp", "列名"),
        "列名", // zero nesting
        (x: Any) => x
      ),
      (
        df.withColumnRenamed("temp", "a"),
        "a", // zero nesting
        (x: Any) => x
      ),
      (
        df.withColumn("a", struct(df("temp") as "b")).drop("temp"),
        "a.b", // one level nesting
        (x: Any) => Row(x)
      ),

      (
        df.withColumn("a", struct(struct(df("temp") as "c") as "b")).drop("temp"),
        "a.b.c", // two level nesting
        (x: Any) => Row(Row(x))
      ),
      (
        df.withColumnRenamed("temp", "a.b"),
        "`a.b`", // zero nesting with column name containing `dots`
        (x: Any) => x
      ),
      (
        df.withColumn("a.b", struct(df("temp") as "c.d")).drop("temp"),
        "`a.b`.`c.d`", // one level nesting with column names containing `dots`
        (x: Any) => Row(x)
      )
    )
  }

  private def testTimestampPushdown(data: Seq[String], java8Api: Boolean): Unit = {
    implicit class StringToTs(s: String) {
      def ts: Timestamp = Timestamp.valueOf(s)
    }
    assert(data.size === 4)
    val ts1 = data.head
    val ts2 = data(1)
    val ts3 = data(2)
    val ts4 = data(3)

    import testImplicits._
    val df = data.map(i => Tuple1(Timestamp.valueOf(i))).toDF()
    withNestedParquetDataFrame(df) { case (parquetDF, colName, fun) =>
      implicit val df: DataFrame = parquetDF

      def resultFun(tsStr: String): Any = {
        val parsed = if (java8Api) {
          LocalDateTime.parse(tsStr.replace(" ", "T"))
            .atZone(ZoneId.systemDefault())
            .toInstant
        } else {
          Timestamp.valueOf(tsStr)
        }
        fun(parsed)
      }

      val tsAttr = df(colName).expr
      assert(df(colName).expr.dataType === TimestampType)

      checkFilterPredicate(tsAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(tsAttr.isNotNull, classOf[NotEq[_]],
        data.map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tsAttr === ts1.ts, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr <=> ts1.ts, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr =!= ts1.ts, classOf[NotEq[_]],
        Seq(ts2, ts3, ts4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tsAttr < ts2.ts, classOf[Lt[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr > ts1.ts, classOf[Gt[_]],
        Seq(ts2, ts3, ts4).map(i => Row.apply(resultFun(i))))
      checkFilterPredicate(tsAttr <= ts1.ts, classOf[LtEq[_]], resultFun(ts1))
      checkFilterPredicate(tsAttr >= ts4.ts, classOf[GtEq[_]], resultFun(ts4))

      checkFilterPredicate(Literal(ts1.ts) === tsAttr, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts1.ts) <=> tsAttr, classOf[Eq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts2.ts) > tsAttr, classOf[Lt[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts3.ts) < tsAttr, classOf[Gt[_]], resultFun(ts4))
      checkFilterPredicate(Literal(ts1.ts) >= tsAttr, classOf[LtEq[_]], resultFun(ts1))
      checkFilterPredicate(Literal(ts4.ts) <= tsAttr, classOf[GtEq[_]], resultFun(ts4))

      checkFilterPredicate(!(tsAttr < ts4.ts), classOf[GtEq[_]], resultFun(ts4))
      checkFilterPredicate(tsAttr < ts2.ts || tsAttr > ts3.ts, classOf[Operators.Or],
        Seq(Row(resultFun(ts1)), Row(resultFun(ts4))))
    }
  }

  // This function tests that exactly go through the `canDrop` and `inverseCanDrop`.
  private def testStringStartsWith(dataFrame: DataFrame, filter: String): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      dataFrame.write.option("parquet.block.size", 512).parquet(path)
      Seq(true, false).foreach { pushDown =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_STARTSWITH_ENABLED.key -> pushDown.toString) {
          val accu = new NumRowGroupsAcc
          sparkContext.register(accu)

          val df = spark.read.parquet(path).filter(filter)
          df.foreachPartition((it: Iterator[Row]) => it.foreach(v => accu.add(0)))
          if (pushDown) {
            assert(accu.value == 0)
          } else {
            assert(accu.value > 0)
          }

          AccumulatorContext.remove(accu.id)
        }
      }
    }
  }

  test("filter pushdown - boolean") {
    val data = (true :: false :: Nil).map(b => Tuple1.apply(Option(b)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val booleanAttr = df(colName).expr
      assert(df(colName).expr.dataType === BooleanType)

      checkFilterPredicate(booleanAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(booleanAttr.isNotNull, classOf[NotEq[_]],
        Seq(Row(resultFun(true)), Row(resultFun(false))))

      checkFilterPredicate(booleanAttr === true, classOf[Eq[_]], resultFun(true))
      checkFilterPredicate(booleanAttr <=> true, classOf[Eq[_]], resultFun(true))
      checkFilterPredicate(booleanAttr =!= true, classOf[NotEq[_]], resultFun(false))
    }
  }

  test("filter pushdown - tinyint") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toByte)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val tinyIntAttr = df(colName).expr
      assert(df(colName).expr.dataType === ByteType)

      checkFilterPredicate(tinyIntAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(tinyIntAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tinyIntAttr === 1.toByte, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr <=> 1.toByte, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr =!= 1.toByte, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(tinyIntAttr < 2.toByte, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr > 3.toByte, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(tinyIntAttr <= 1.toByte, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(tinyIntAttr >= 4.toByte, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1.toByte) === tinyIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1.toByte) <=> tinyIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2.toByte) > tinyIntAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3.toByte) < tinyIntAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1.toByte) >= tinyIntAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4.toByte) <= tinyIntAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(tinyIntAttr < 4.toByte), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(tinyIntAttr < 2.toByte || tinyIntAttr > 3.toByte,
        classOf[Operators.Or], Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - smallint") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toShort)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val smallIntAttr = df(colName).expr
      assert(df(colName).expr.dataType === ShortType)

      checkFilterPredicate(smallIntAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(smallIntAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(smallIntAttr === 1.toShort, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr <=> 1.toShort, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr =!= 1.toShort, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(smallIntAttr < 2.toShort, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr > 3.toShort, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(smallIntAttr <= 1.toShort, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(smallIntAttr >= 4.toShort, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1.toShort) === smallIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1.toShort) <=> smallIntAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2.toShort) > smallIntAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3.toShort) < smallIntAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1.toShort) >= smallIntAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4.toShort) <= smallIntAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(smallIntAttr < 4.toShort), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(smallIntAttr < 2.toShort || smallIntAttr > 3.toShort,
        classOf[Operators.Or], Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - integer") {
    val data = (1 to 4).map(i => Tuple1(Option(i)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val intAttr = df(colName).expr
      assert(df(colName).expr.dataType === IntegerType)

      checkFilterPredicate(intAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(intAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(intAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(intAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(intAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(intAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(intAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(intAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(intAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === intAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> intAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > intAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < intAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= intAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= intAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(intAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(intAttr < 2 || intAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - long") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toLong)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val longAttr = df(colName).expr
      assert(df(colName).expr.dataType === LongType)

      checkFilterPredicate(longAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(longAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(longAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(longAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(longAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(longAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(longAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(longAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(longAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === longAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> longAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > longAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < longAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= longAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= longAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(longAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(longAttr < 2 || longAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - float") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toFloat)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val floatAttr = df(colName).expr
      assert(df(colName).expr.dataType === FloatType)

      checkFilterPredicate(floatAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(floatAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(floatAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(floatAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(floatAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(floatAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(floatAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(floatAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(floatAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === floatAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> floatAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > floatAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < floatAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= floatAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= floatAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(floatAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(floatAttr < 2 || floatAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - double") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toDouble)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val doubleAttr = df(colName).expr
      assert(df(colName).expr.dataType === DoubleType)

      checkFilterPredicate(doubleAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(doubleAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(doubleAttr === 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr <=> 1, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr =!= 1, classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i))))

      checkFilterPredicate(doubleAttr < 2, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(doubleAttr > 3, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(doubleAttr <= 1, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(doubleAttr >= 4, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(Literal(1) === doubleAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(1) <=> doubleAttr, classOf[Eq[_]], resultFun(1))
      checkFilterPredicate(Literal(2) > doubleAttr, classOf[Lt[_]], resultFun(1))
      checkFilterPredicate(Literal(3) < doubleAttr, classOf[Gt[_]], resultFun(4))
      checkFilterPredicate(Literal(1) >= doubleAttr, classOf[LtEq[_]], resultFun(1))
      checkFilterPredicate(Literal(4) <= doubleAttr, classOf[GtEq[_]], resultFun(4))

      checkFilterPredicate(!(doubleAttr < 4), classOf[GtEq[_]], resultFun(4))
      checkFilterPredicate(doubleAttr < 2 || doubleAttr > 3, classOf[Operators.Or],
        Seq(Row(resultFun(1)), Row(resultFun(4))))
    }
  }

  test("filter pushdown - string") {
    val data = (1 to 4).map(i => Tuple1(Option(i.toString)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val stringAttr = df(colName).expr
      assert(df(colName).expr.dataType === StringType)

      checkFilterPredicate(stringAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
      checkFilterPredicate(stringAttr.isNotNull, classOf[NotEq[_]],
        (1 to 4).map(i => Row.apply(resultFun(i.toString))))

      checkFilterPredicate(stringAttr === "1", classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr <=> "1", classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr =!= "1", classOf[NotEq[_]],
        (2 to 4).map(i => Row.apply(resultFun(i.toString))))

      checkFilterPredicate(stringAttr < "2", classOf[Lt[_]], resultFun("1"))
      checkFilterPredicate(stringAttr > "3", classOf[Gt[_]], resultFun("4"))
      checkFilterPredicate(stringAttr <= "1", classOf[LtEq[_]], resultFun("1"))
      checkFilterPredicate(stringAttr >= "4", classOf[GtEq[_]], resultFun("4"))

      checkFilterPredicate(Literal("1") === stringAttr, classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(Literal("1") <=> stringAttr, classOf[Eq[_]], resultFun("1"))
      checkFilterPredicate(Literal("2") > stringAttr, classOf[Lt[_]], resultFun("1"))
      checkFilterPredicate(Literal("3") < stringAttr, classOf[Gt[_]], resultFun("4"))
      checkFilterPredicate(Literal("1") >= stringAttr, classOf[LtEq[_]], resultFun("1"))
      checkFilterPredicate(Literal("4") <= stringAttr, classOf[GtEq[_]], resultFun("4"))

      checkFilterPredicate(!(stringAttr < "4"), classOf[GtEq[_]], resultFun("4"))
      checkFilterPredicate(stringAttr < "2" || stringAttr > "3", classOf[Operators.Or],
        Seq(Row(resultFun("1")), Row(resultFun("4"))))
    }
  }

  // TODO:  results of unicode string filter are not correct

  //  test("filter pushdown - unicode string") {
  //    def toUnicodeChar(i: Int) = {
  //      new StringBuffer().append("\\u" + (8544 - 1 + i).toHexString).toString
  //    }
  //
  //    val data = (1 to 4).map(i => Tuple1(Option(toUnicodeChar(i))))
  //    println(data)
  //    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
  //      implicit val df: DataFrame = inputDF
  //
  //      val stringAttr = df(colName).expr
  //      assert(df(colName).expr.dataType === StringType)
  //
  //      checkFilterPredicate(stringAttr.isNull, classOf[Eq[_]], Seq.empty[Row])
  //      checkFilterPredicate(stringAttr.isNotNull, classOf[NotEq[_]],
  //        (1 to 4).map(i => Row.apply(resultFun(toUnicodeChar(i)))))
  //
  //      checkFilterPredicate(stringAttr === "\u2160", classOf[Eq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(stringAttr <=> "\u2160", classOf[Eq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(stringAttr =!= "\u2160", classOf[NotEq[_]],
  //        (2 to 4).map(i => Row.apply(resultFun(toUnicodeChar(i)))))
  //
  //      checkFilterPredicate(stringAttr < "\u2161", classOf[Lt[_]], resultFun("\u2160"))
  //      checkFilterPredicate(stringAttr > "\u2162", classOf[Gt[_]], resultFun("\u2163"))
  //      checkFilterPredicate(stringAttr <= "\u2160", classOf[LtEq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(stringAttr >= "\u2163", classOf[GtEq[_]], resultFun("\u2163"))
  //
  //      checkFilterPredicate(Literal("\u2160") === stringAttr, classOf[Eq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(Literal("\u2160") <=> stringAttr, classOf[Eq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(Literal("\u2161") > stringAttr, classOf[Lt[_]], resultFun("\u2160"))
  //      checkFilterPredicate(Literal("\u2162") < stringAttr, classOf[Gt[_]], resultFun("\u2163"))
  //      checkFilterPredicate(Literal("\u2160") >= stringAttr, classOf[LtEq[_]], resultFun("\u2160"))
  //      checkFilterPredicate(Literal("\u2163") <= stringAttr, classOf[GtEq[_]], resultFun("\u2163"))
  //
  //      checkFilterPredicate(!(stringAttr < "\u2163"), classOf[GtEq[_]], resultFun("\u2163"))
  //      checkFilterPredicate(stringAttr < "\u2161" || stringAttr > "\u2162", classOf[Operators.Or],
  //        Seq(Row(resultFun("\u2160")), Row(resultFun("\u2163"))))
  //    }
  //  }

  test("filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes(StandardCharsets.UTF_8)
    }

    val data = (1 to 4).map(i => Tuple1(Option(i.b)))
    withNestedParquetDataFrame(data) { case (inputDF, colName, resultFun) =>
      implicit val df: DataFrame = inputDF

      val binaryAttr: Expression = df(colName).expr
      assert(df(colName).expr.dataType === BinaryType)
