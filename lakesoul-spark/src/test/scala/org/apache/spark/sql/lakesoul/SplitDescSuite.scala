package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.jnr.NativeMetadataJavaClient
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
import org.apache.spark.sql.lakesoul.RandomStringGenerator.generateRandomString
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SplitDescSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

  import testImplicits._

  val names: Seq[String] = Seq.empty
  val basePath = "/tmp/spark_test"
  val nameLength = 10

  private def create_dataframe(): DataFrame = {
    val df = Seq(
      ("2021-01-01", 1, 1, "apple"),
      ("2021-01-01", 2, 2, "banana"),
      ("2021-01-02", 3, 3, "pear"),
      ("2021-01-02", 4, 4, "lemon"),
      ("2021-01-03", 5, 5, "watermelon"),
      ("2021-01-03", 6, 6, "grape"),
      ("2021-01-04", 7, 7, "cherry"),
      ("2021-01-04", 8, 8, "pineapple"),
    ).toDF("date", "id", "num", "name")
    df
  }

  private def withUpsert(tableName: String): Unit = {

    val df1 = Seq(
      ("2021-01-01", 1, 1, "apple"),
      ("2021-01-02", 2, 2, "banana"),
    ).toDF("date", "id", "num", "name")

    val tablePath = s"$basePath/$tableName"

    df1.write
      .mode("append")
      .format("lakesoul")
      .option("shortTableName", tableName)
      .option("rangePartitions", "date")
      .option("hashPartitions", "id,num")
      .option("hashBucketNum", "4")
      .save(tablePath)

    val lake = LakeSoulTable.forPath(tablePath)

    val df2 = Seq(
      ("2021-01-01", 1, 1, "pear"),
      ("2021-01-02", 2, 2, "lemon"),
    ).toDF("date", "id", "num", "name")

    lake.upsert(df2)

    val df3 = Seq(
      ("2021-01-01", 1, 2, "watermelon"),
      ("2021-01-02", 1, 2, "grape"),
    ).toDF("date", "id", "num", "name")

    lake.upsert(df3)

    val df4 = Seq(
      ("2021-01-01", 1, 1, "cherry"),
      ("2021-01-02", 2, 2, "pineapple"),
    ).toDF("date", "id", "num", "name")

    lake.upsert(df4)
  }


  test("no range, no hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toArray
      descs.foreach(println)
      assert(descs.length == 1)
      val desc = descs(0);
      assert(!desc.getFilePaths.isEmpty)
      assert(desc.getPrimaryKeys.isEmpty)
      assert(desc.getPartitionDesc.isEmpty)
    }
  }

  test("one range, no hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("rangePartitions", "date")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 4)
    }
  }

  test("multiple range, no hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("rangePartitions", "date,name")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 8)
    }
  }

  test("no range, one hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("hashPartitions", "id")
        .option("hashBucketNum", "4")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 3)
    }
  }

  test("one range, one hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "4")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 7)
    }
  }

  test("multiple range, one hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("rangePartitions", "date,name")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "4")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 8)
    }
  }

  test("multiple range, multiple hash") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      val tablePath = s"$basePath/$tName"
      val df = create_dataframe()
      df.write
        .mode("append")
        .format("lakesoul")
        .option("shortTableName", tName)
        .option("rangePartitions", "date,name")
        .option("hashPartitions", "id,num")
        .option("hashBucketNum", "4")
        .save(tablePath)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
      assert(descs.length == 8)
    }
  }


  test("multiple range, multiple hash , with upsert") {
    val tName = generateRandomString(nameLength);
    withTable(tName) {
      withUpsert(tName)
      val descs = NativeMetadataJavaClient
        .getInstance()
        .createSplitDescArray(tName, "default")
        .asScala
        .toSeq
      descs.foreach(println)
    }
  }
}


object RandomStringGenerator {
  val random = new Random()

  def generateRandomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    val sb = new StringBuilder
    for (_ <- 1 to length) {
      val randomIndex = random.nextInt(chars.length)
      sb.append(chars(randomIndex))
    }
    sb.toString()
  }
}
