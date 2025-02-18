// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.ml.lakesoul.scanns.algorithm.L2ScalarRandomProjectionNNS
//import org.apache.spark.ml.lakesoul.scanns.algorithm.L2ScalarRandomProjectionNNS
import io.jhdf.HdfFile
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestBeforeAndAfterEach, LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.util.Utils
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatestplus.junit.JUnitRunner
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.functions.{collect_list, sum, udf}
import org.apache.spark.sql.types.{ArrayType, ByteType, FloatType, IntegerType, LongType, MetadataBuilder, StructField, StructType}

import scala.math.{pow, sqrt}
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class ANNCase extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach
  with LakeSoulTestUtils with LakeSoulSQLCommandTest {

  val testDatasetSize = 10000
  val numQuery = 10
  val topK = 100
  val sampleIndices = scala.util.Random.shuffle((0 until testDatasetSize).toList).take(numQuery)

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

  test("test LakeSoulHammingDistanceLSH") {
    withTempDir { trainDir =>
      withTempDir { testDir =>
        val filepath = "/Users/ceng/Downloads/fashion-mnist-784-euclidean.hdf5"
        val trainPath = trainDir.getCanonicalPath
        val testPath = testDir.getCanonicalPath

        println(filepath)
        val spark = SparkSession.builder
          .appName("Array to RDD")
          .master("local[*]")
          .getOrCreate()
        try {
          val hdfFile = new HdfFile(Paths.get(filepath))

          val trainDataset = hdfFile.getDatasetByPath("train")
          val testDataset = hdfFile.getDatasetByPath("test")
          val neighborDataset = hdfFile.getDatasetByPath("neighbors")
          val trainData = trainDataset.getData()
          val testData = testDataset.getData()
          val neighborData = neighborDataset.getData()

          var float2DDataNeighbor: Array[Array[Int]] = null
          neighborData match {
            case data: Array[Array[Int]] =>
              float2DDataNeighbor = data
            case _ =>
              println("not")
          }
          // the smaller the Hamming distance,the greater the similarity
          val calculateHammingDistanceUDF = udf((trainLSH: Seq[Long], testLSH: Seq[Long]) => {
            require(trainLSH.length == testLSH.length, "The input sequences must have the same length")
            trainLSH.zip(testLSH).map { case (train, test) =>
              java.lang.Long.bitCount(train ^ test)
            }.sum
          })
          // the smaller the Euclidean distance,the greater the similarity
          val calculateEuclideanDistanceUDF = udf((trainEmbedding: Seq[Float], testEmbedding: Seq[Float]) => {
            require(testEmbedding.length == trainEmbedding.length, "The input sequences must have the same length")
            sqrt(trainEmbedding.zip(testEmbedding).map { case (train, test) =>
              pow(train - test, 2)
            }.sum)
          })
          //the greater the Cosine distance,the greater the similarity
          val calculateCosineDistanceUDF = udf((trainEmbedding: Seq[Float], testEmbedding: Seq[Float]) => {
            require(testEmbedding.length == trainEmbedding.length, "The input sequences must have the same length")
            trainEmbedding.zip(testEmbedding).map { case (train, test) =>
              train * test
            }.sum / (sqrt(trainEmbedding.map { train => train * train }.sum) * sqrt(testEmbedding.map { test => test * test }.sum))
          })
          //the smaller the Jaccard distance,the greater the similarity
          val calculateJaccardDistanceUDF = udf((trainEmbedding: Seq[Float], testEmbedding: Seq[Float]) => {
            require(testEmbedding.length == trainEmbedding.length, "The input sequences must have the same length")
            val anb = testEmbedding.intersect(trainEmbedding).distinct
            val aub = testEmbedding.union(trainEmbedding).distinct
            val jaccardCoefficient = anb.length.toDouble / aub.length
            1 - jaccardCoefficient
          })
          spark.udf.register("calculateHammingDistance", calculateHammingDistanceUDF)
          spark.udf.register("calculateEuclideanDistance", calculateEuclideanDistanceUDF)
          spark.udf.register("calculateCosineDistance", calculateCosineDistanceUDF)
          spark.udf.register("calculateJaccardDistance", calculateJaccardDistanceUDF)
          //      println(float2DDataNeighbor.length)
          val schema = StructType(Array(
            StructField("IndexId", IntegerType, true),
            StructField("Embedding", ArrayType(FloatType), true, new MetadataBuilder()
              .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_EMBEDDING_DIMENSION, "784")
              .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_BIT_WIDTH, "784")
              .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_RNG_SEED, "1234567890").build()),
            StructField("Embedding_LSH", ArrayType(LongType), true)
          ))
          trainData match {
            case float2DData: Array[Array[Float]] =>
              val trainSize = float2DData.length
              val classIds = float2DData.indices.toArray

              val rows = float2DData.zip(classIds).map {
                case (embedding, indexId) =>
                  //                  Row(indexId, embedding)
                  Row(indexId, embedding, null)
              }
              val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
              df.write.format("lakesoul")
                .option("hashPartitions", "IndexId")
                .option("hashBucketNum", 4)
                .option(LakeSoulOptions.SHORT_TABLE_NAME, "trainData")
                .mode("Overwrite").save(trainPath)

              //            val startTime1 = System.nanoTime()
              val lakeSoulTable = LakeSoulTable.forPath(trainPath)
              lakeSoulTable.compaction()

              testData match {
                case float2DTestData: Array[Array[Float]] =>


                  val dfTest = spark.createDataFrame(spark.sparkContext.parallelize(
                    sampleIndices.map(index => Row(index, float2DTestData(index), null))
                  ), schema).limit(numQuery)

                  dfTest.write.format("lakesoul")
                    .option("hashPartitions", "IndexId")
                    .option("hashBucketNum", 4)
                    .option(LakeSoulOptions.SHORT_TABLE_NAME, "testData")
                    .mode("Overwrite").save(testPath)

                  val n = 30
                  val topkFirst = trainSize / 5

                  val result = spark.time({

                    val rank = spark.sql(
                      s"""
                    SELECT *
                    FROM (
                        SELECT
                            testData.IndexId AS indexIdTest,
                            trainData.IndexId AS indexIdTrain,
                            testData.Embedding as EmbeddingTest,
                            trainData.Embedding as EmbeddingTrain,
                            ROW_NUMBER() OVER(PARTITION BY testData.IndexId ORDER BY calculateHammingDistance(testData.Embedding_LSH, trainData.Embedding_LSH) asc) AS rank
                        FROM testData
                        CROSS JOIN trainData
                    ) ranked
                    WHERE rank <= $topkFirst
                """)
                    rank.createOrReplaceTempView("rank")
                    //                    spark.sql("select * from rank limit 100").show(100)
                    val reResult = spark.sql(
                      s"""
                     SELECT *
                     FROM (
                        SELECT
                          rank.indexIdTest,
                          rank.indexIDTrain,
                          calculateEuclideanDistance(rank.EmbeddingTest,rank.EmbeddingTrain) as distance
                        FROM rank
                     ) reRanked
                    ORDER BY distance asc
                     """).select($"indexIdTest".cast(LongType), $"indexIdTrain".cast(LongType), $"distance")
                    reResult.collect()
                  })


                  //                  val sampleIndices = (0 until numQuery).toSeq
                  val recall = calculateRecall(sampleIndices, float2DDataNeighbor, result, topK, numQuery)
                  println(s"average recall = $recall")

              }
          }
        }
        finally {

        }
      }
    }
  }

  // 添加通用的召回率计算函数
  private def calculateRecall(
                               sampleIndices: Seq[Int],
                               trueNeighbors: Array[Array[Int]],
                               predictedNeighbors: Array[Row],
                               k: Int,
                               numSamples: Int): Double = {
    var totalRecall = 0.0
    for (i <- sampleIndices.indices) {
      println(sampleIndices(i))
      val trueSet = trueNeighbors(sampleIndices(i)).take(k).toSet
      println(trueSet)
      val predictedSet = predictedNeighbors
        .filter(_.getAs[Long](0) == sampleIndices(i))
        .map(_.getAs[Long](1).toInt)
        .take(k)
        .toSet
      println(predictedSet)

      val intersection = trueSet.intersect(predictedSet)
      totalRecall += intersection.size.toDouble / k
    }
    totalRecall / numSamples
  }

  test("test spark BucketedRandomProjectionLSH on MNIST") {
    val hdfFile = new HdfFile(Paths.get("/Users/ceng/Downloads/fashion-mnist-784-euclidean.hdf5"))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]]
    val testVectors = testData.asInstanceOf[Array[Array[Float]]]
    val trueNeighbors = neighborData.asInstanceOf[Array[Array[Int]]]

    val numFeatures = trainVectors(0).length

    val brp = new BucketedRandomProjectionLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(10)
      .setSeed(1234)
      .setBucketLength(numFeatures)


    // 将sampledTestVectors转换为RDD[(Long, Vector)]格式
    val sampledTestRDD = spark.sparkContext.parallelize(
      sampleIndices.map { case idx =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
      }
    )

    // 将candidatePool转换为RDD[(Long, Vector)]格式
    val candidateRDD = spark.sparkContext.parallelize(
      trainVectors.zipWithIndex.map { case (vec, idx) =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
      }
    )

    // 将数据转换为DataFrame格式
    val sampledTestDF = sampledTestRDD.toDF("id", "features")
    val candidateDF = candidateRDD.toDF("id", "features")
    //    sampledTestDF.show(truncate = false)
    //    candidateDF.show(truncate = false)

    // 训练LSH模型
    val model = brp.fit(candidateDF.union(sampledTestDF))

    // 定义k值（最近邻的数量）


    val predictedNeighbors =
      spark.time({
        // 使用模型进行近似最近邻搜索
        val predictedNeighborsDF = model.approxSimilarityJoin(
          sampledTestDF,
          candidateDF,
          numFeatures * 3, // threshold
          "distCol"
        ).select($"datasetA.id", $"datasetB.id", $"distCol")
          .orderBy($"distCol")

        //    predictedNeighborsDF.show()
        predictedNeighborsDF.collect()
      })

    // 使用新的通用函数计算召回率
    val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighbors, topK, numQuery)
    println(s"Average recall@$topK = $avgRecall")
  }

  test("test LinkedIn LSH on MNIST") {
    val hdfFile = new HdfFile(Paths.get("/Users/ceng/Downloads/fashion-mnist-784-euclidean.hdf5"))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]]
    val testVectors = testData.asInstanceOf[Array[Array[Float]]]
    val trueNeighbors = neighborData.asInstanceOf[Array[Array[Int]]]

    val sampledTestRDD = spark.sparkContext.parallelize(
      sampleIndices.map { case idx =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
      }
    )

    val candidateRDD = spark.sparkContext.parallelize(
      trainVectors.zipWithIndex.map { case (vec, idx) =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
      }
    )

    val numFeatures = trainVectors(0).length

    val model = new L2ScalarRandomProjectionNNS()
      .setNumHashes(512)
      .setSignatureLength(16)
      //      .setJoinParallelism(5000)
      //      .setBucketLimit(1000)
      .setBucketWidth(trainVectors.length / 8)
      //      .setShouldSampleBuckets(true)
      //      .setNumOutputPartitions(100)
      .createModel(numFeatures)

    val result: Array[(Long, Long, Double)] = spark.time(
      model.getAllNearestNeighbors(sampledTestRDD, candidateRDD, topK).collect()
    )
    // 将结果转换为Row格式
    val predictedNeighborsAsRows = result.map { case (queryId, neighborId, distance) =>
      Row(queryId, neighborId, distance)
    }

    // 计算召回率
    val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
    println(s"Average recall@$topK = $avgRecall")
  }

  test("test LakeSoul BucketedRandomProjectionLSH on MNIST") {
    val hdfFile = new HdfFile(Paths.get("/Users/ceng/Downloads/fashion-mnist-784-euclidean.hdf5"))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]]
    val testVectors = testData.asInstanceOf[Array[Array[Float]]]
    val trueNeighbors = neighborData.asInstanceOf[Array[Array[Int]]]

    val numFeatures = trainVectors(0).length

    val brp = new org.apache.spark.ml.lakesoul.feature.BucketedRandomProjectionLSH()
      .setInputCol("features")
      .setOutputCol("hashes")
      .setNumHashTables(10)
      .setSeed(1234)
      .setBucketLength(numFeatures)


    // 将sampledTestVectors转换为RDD[(Long, Vector)]格式
    val sampledTestRDD = spark.sparkContext.parallelize(
      sampleIndices.map { case idx =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
      }
    )

    // 将candidatePool转换为RDD[(Long, Vector)]格式
    val candidateRDD = spark.sparkContext.parallelize(
      trainVectors.zipWithIndex.map { case (vec, idx) =>
        (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
      }
    )

    // 将数据转换为DataFrame格式
    val sampledTestDF = sampledTestRDD.toDF("id", "features")
    val candidateDF = candidateRDD.toDF("id", "features")
    //    sampledTestDF.show(truncate = false)
    //    candidateDF.show(truncate = false)

    // 训练LSH模型
    val model = brp.fit(candidateDF.union(sampledTestDF))

    // 定义k值（最近邻的数量）


    val predictedNeighbors =
      spark.time({
        // 使用模型进行近似最近邻搜索
        val predictedNeighborsDF = model.approxSimilarityJoin(
          sampledTestDF,
          candidateDF,
          numFeatures * 3, // threshold
          "distCol"
        ).select($"datasetA.id", $"datasetB.id", $"distCol")
          .orderBy($"distCol")

        //    predictedNeighborsDF.show()
        predictedNeighborsDF.collect()
      })

    // 使用新的通用函数计算召回率
    val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighbors, topK, numQuery)
    println(s"Average recall@$topK = $avgRecall")
  }
}
