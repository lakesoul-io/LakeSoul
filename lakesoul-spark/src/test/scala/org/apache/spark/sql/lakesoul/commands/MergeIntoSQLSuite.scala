// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.LakeSoulOptions
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestBeforeAndAfterEach, LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.util.Utils
import org.scalatest._
import matchers.should.Matchers._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import io.jhdf.HdfFile
import io.jhdf.api.Dataset
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.sql.types.{ArrayType, ByteType, FloatType, IntegerType, LongType, StructField, StructType}
import org.apache.commons.lang3.ArrayUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{collect_list, sum, udf}

import java.nio.file.Paths
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationLong
import scala.math.{pow, sqrt}

@RunWith(classOf[JUnitRunner])
class MergeIntoSQLSuite extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach
  with LakeSoulTestUtils with LakeSoulSQLCommandTest {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  import testImplicits._

  protected def initTable(df: DataFrame,
                          rangePartition: Seq[String] = Nil,
                          hashPartition: Seq[String] = Nil,
                          hashBucketNum: Int = 2): Unit = {
    val writer = df.write.format("lakesoul").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition.mkString(","))
      .option("hashPartitions", hashPartition.mkString(","))
      .option("hashBucketNum", hashBucketNum)
      .save(snapshotManagement.table_path)
  }

  private def initHashTable(): Unit = {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      Seq("range"),
      Seq("hash")
    )
  }

  private def withViewNamed(df: DataFrame, viewName: String)(f: => Unit): Unit = {
    df.createOrReplaceTempView(viewName)
    Utils.tryWithSafeFinally(f) {
      spark.catalog.dropTempView(viewName)
    }
  }

//  test("test lsh"){
//    val filepath = "/Users/beidu/Documents/dataset/glove-200-angular.hdf5"
//    val trainPath = "/Users/beidu/Documents/LakeSoul/train"
//    val testPath = "/Users/beidu/Documents/LakeSoul/test"
//
//    println(filepath)
//    val spark = SparkSession.builder
//      .appName("Array to RDD")
//      .master("local[*]")
//      .getOrCreate()
//    try{
//      val hdfFile = new HdfFile(Paths.get(filepath))
//
//      val trainDataset = hdfFile.getDatasetByPath("train")
//      val testDataset = hdfFile.getDatasetByPath("test")
//      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
//      val trainData = trainDataset.getData()
//      val testData = testDataset.getData()
//      val neighborData = neighborDataset.getData()
//      println(trainData)
//      var float2DDataNeighbor: Array[Array[Int]] = null
//      neighborData match {
//        case data:Array[Array[Int]] =>
//          float2DDataNeighbor = data
//        case _ =>
//          println("not")
//      }
//      // the smaller the Hamming distance,the greater the similarity
//      val calculateHammingDistanceUDF = udf((trainLSH: Seq[Long], testLSH: Seq[Long]) => {
//        require(trainLSH.length == testLSH.length, "The input sequences must have the same length")
//        trainLSH.zip(testLSH).map { case (train, test) =>
//          java.lang.Long.bitCount(train ^ test)
//        }.sum
//      })
//      // the smaller the Euclidean distance,the greater the similarity
//      val calculateEuclideanDistanceUDF = udf((trainEmbedding:Seq[Float],testEmbedding:Seq[Float]) => {
//        require(testEmbedding.length == trainEmbedding.length,"The input sequences must have the same length")
//        sqrt(trainEmbedding.zip(testEmbedding).map{case (train,test) =>
//          pow(train - test,2) }.sum)
//      })
//      //the greater the Cosine distance,the greater the similarity
//      val calculateCosineDistanceUDF = udf((trainEmbedding:Seq[Float],testEmbedding:Seq[Float]) => {
//        require(testEmbedding.length == trainEmbedding.length,"The input sequences must have the same length")
//        trainEmbedding.zip(testEmbedding).map{case (train,test) =>
//          train * test}.sum / (sqrt(trainEmbedding.map{train => train * train}.sum) * sqrt(testEmbedding.map{test => test * test}.sum))
//      })
//      //the smaller the Jaccard distance,the greater the similarity
//      val calculateJaccardDistanceUDF = udf((trainEmbedding:Seq[Float],testEmbedding:Seq[Float]) => {
//        require(testEmbedding.length == trainEmbedding.length,"The input sequences must have the same length")
//        val anb = testEmbedding.intersect(trainEmbedding).distinct
//        val aub = testEmbedding.union(trainEmbedding).distinct
//        val jaccardCoefficient = anb.length.toDouble / aub.length
//        1 - jaccardCoefficient
//      })
//      spark.udf.register("calculateHammingDistance",calculateHammingDistanceUDF)
//      spark.udf.register("calculateEuclideanDistance",calculateEuclideanDistanceUDF)
//      spark.udf.register("calculateCosineDistance",calculateCosineDistanceUDF)
//      spark.udf.register("calculateJaccardDistance",calculateJaccardDistanceUDF)
////      println(float2DDataNeighbor.length)
//      trainData match {
//        case float2DData:Array[Array[Float]] =>
//            val classIds = (1 to float2DData.length).toArray
//            val schema = StructType(Array(
//              StructField("IndexId",IntegerType,true),
//              StructField("Embedding",ArrayType(FloatType),true),
//              StructField("LSH",ArrayType(LongType),true)
//            ))
//            val rows = float2DData.zip(classIds).map {
//              case (embedding,indexId)=>
//                Row(indexId,embedding,null)
//            }
//            val df = spark.createDataFrame(spark.sparkContext.parallelize(rows),schema)
//            df.write.format("lakesoul")
//              .option("hashPartitions", "IndexId")
//              .option("hashBucketNum", 4)
//              .option(LakeSoulOptions.SHORT_TABLE_NAME,"trainData")
//              .mode("Overwrite").save(trainPath)
////            val startTime1 = System.nanoTime()
//            val lakeSoulTable = LakeSoulTable.forPath(trainPath)
//            lakeSoulTable.compaction()
//
//            testData match {
//              case float2DTestData:Array[Array[Float]] =>
//                val classIdsTest = (1 to float2DTestData.length).toArray
//                val schemaTest = StructType(Array(
//                  StructField("IndexId",IntegerType,true),
//                  StructField("Embedding",ArrayType(FloatType),true),
//                  StructField("LSH",ArrayType(LongType),true)
//                ))
//                val rowsTest = float2DTestData.zip(classIdsTest).map{
//                  case (embedding,indexId) =>
//                    Row(indexId,embedding,null)
//                }
//
//                val num = 50
//                val dfTest = spark.createDataFrame(spark.sparkContext.parallelize(rowsTest),schemaTest).limit(num)
//                dfTest.write.format("lakesoul")
//                .option("hashPartitions","IndexId")
//                .option("hashBucketNum",4)
//                .option(LakeSoulOptions.SHORT_TABLE_NAME,"testData")
//                .mode("Overwrite").save(testPath)
//                val lakeSoulTableTest = LakeSoulTable.forPath(testPath)
//
//                lakeSoulTableTest.compaction()
////                val endTime1 = System.nanoTime()
////                val duration1 = (endTime1 - startTime1).nanos
////                println(s"time:${duration1.toMillis}")
////                val lshTrain = sql("select LSH from trainData")
////                val lshTest = sql("select LSH from testData")
//
////                val arr = Array(1,5,10,20,40,60,80,100,150,200,250,300)
////                for(n <- arr) {
//                  val n = 300
//                  val topk = 100
//                  val topkFirst = n * topk
//
//                  //                val result = sql("select testData.IndexId as indexId,trainData.LSH as trainLSH,testData.LSH as testLSH," +
//                  //                  "calculateHammingDistance(testData.LSH,trainData.LSH) AS hamming_distance " +
//                  //                  "from testData " +
//                  //                  "cross join trainData " +
//                  //                  "order by indexId,hamming_distance")
//
//                  //                val result = spark.sql(s"""
//                  //                    SELECT *
//                  //                    FROM (
//                  //                        SELECT
//                  //                            testData.IndexId AS indexIdTest,
//                  //                            trainData.IndexId AS indexIdTrain,
//                  //                            trainData.LSH AS trainLSH,
//                  //                            testData.LSH AS testLSH,
//                  //                            calculateHammingDistance(testData.LSH, trainData.LSH) AS hamming_distance,
//                  //                            ROW_NUMBER() OVER (PARTITION BY testData.IndexId ORDER BY calculateHammingDistance(testData.LSH, trainData.LSH) asc) AS rank
//                  //                        FROM testData
//                  //                        CROSS JOIN trainData
//                  //                    ) ranked
//                  //                    WHERE rank <= $topk
//                  //                """).groupBy("indexIdTest").agg(collect_list("indexIdTrain").alias("indexIdTrainList"))
//                  val startTime = System.nanoTime()
//                  val result = spark.sql(
//                    s"""
//                  SELECT *
//                  FROM (
//                      SELECT
//                          testData.IndexId AS indexIdTest,
//                          trainData.IndexId AS indexIdTrain,
//                          testData.Embedding as EmbeddingTest,
//                          trainData.Embedding as EmbeddingTrain,
//                          ROW_NUMBER() OVER (PARTITION BY testData.IndexId ORDER BY calculateHammingDistance(testData.LSH, trainData.LSH) asc) AS rank
//                      FROM testData
//                      CROSS JOIN trainData
//                  ) ranked
//                  WHERE rank <= $topkFirst
//              """)
//                  result.createOrReplaceTempView("rank")
//                  val reResult = spark.sql(
//                    s"""
//                   SELECT *
//                   FROM (
//                      SELECT
//                        rank.indexIdTest,
//                        rank.indexIDTrain,
//                        ROW_NUMBER() OVER(PARTITION BY rank.indexIdTest ORDER BY calculateEuclideanDistance(rank.EmbeddingTest,rank.EmbeddingTrain) asc) AS reRank
//                      FROM rank
//                   ) reRanked
//                   WHERE reRank <= $topk
//                   """).groupBy("indexIdTest").agg(collect_list("indexIdTrain").alias("indexIdTrainList"))
//
//
//                  val endTime = System.nanoTime()
//                  val duration = (endTime - startTime).nanos
//                  println(s"time for query n4topk ${n} :${duration.toMillis} milliseconds")
//
//                  val startTime2 = System.nanoTime()
//
////                  val (totalRecall, count) = reResult.map(row => {
////                    val indexIdTest = row.getAs[Int]("indexIdTest")
////                    val indexIdTrainList: Array[Int] = row.getAs[Seq[Int]]("indexIdTrainList").toArray
////                    val updatedList = indexIdTrainList.map(_ - 1)
////                    val count = float2DDataNeighbor(indexIdTest - 1).take(topk).count(updatedList.contains)
////                    val recall = (count * 1.0 / topk)
////                    (recall, 1)
////                  }).reduce((acc1, acc2) => {
////                    (acc1._1 + acc2._1, acc1._2 + acc2._2)
////                  })
////                  println(totalRecall / count)
//                  val endTime2 = System.nanoTime()
//                  val duration2 = (endTime2 - startTime2).nanos
//                  println(s"time for sort:${duration2.toMillis} milliseconds")
////                }
//              }
//              case _ =>
//                println("unexpected data type")
//        case _ =>
//          println("unexpected data type")
//      }
//    }
//    finally {
//
//    }
//
//  }

  test("merge into table with hash partition -- supported case") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
        s" ON t.hash = s.hash" +
        s" WHEN MATCHED THEN UPDATE SET *" +
        s" WHEN NOT MATCHED THEN INSERT *")
      checkAnswer(readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
        Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(20201101, 3, 3) :: Row(20201102, 4, 5) :: Nil)
    }
  }

  test("merge into table with hash partition -- invalid merge condition") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      val e = intercept[AnalysisException] {
        sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.value = s.value" +
          s" WHEN MATCHED THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *")
      }
      e.getMessage() should (include("Convert merge into to upsert with merge condition") and include("is not supported"))
    }
  }

  test("merge into table with hash partition -- invalid matched condition") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      val e = intercept[AnalysisException] {
        sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.hash = s.hash" +
          s" WHEN MATCHED AND t.VALUE=5 THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *")
      }
      e.getMessage() should (include("Convert merge into to upsert with MatchedAction") and include("is not supported"))
    }
  }
}
