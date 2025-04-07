// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.ml.lakesoul.scanns.algorithm.L2ScalarRandomProjectionNNS
import org.apache.spark.ml.lakesoul.scanns.algorithm.LakeSoulRandomProjectionNNS
import org.apache.spark.ml.lakesoul.scanns.model.LakeSoulRandomProjectionModel
import org.apache.spark.ml.lakesoul.scanns.model.LakeSoulLSHNearestNeighborSearchModel
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
import org.apache.spark.sql.functions.{col, collect_list, lit, sum, udf}
import org.apache.spark.sql.types.{ArrayType, ByteType, DoubleType, FloatType, IntegerType, LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.ml.lakesoul.utils.DataNormalizer
import org.apache.spark.ml.lakesoul.utils.DataNormalizer._
import org.apache.spark.sql.lakesoul.LakeSoulTableProperties.skipMergeOnRead

import scala.math.{pow, sqrt}
import java.nio.file.Paths

@RunWith(classOf[JUnitRunner])
class ANNCase extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach
  with LakeSoulTestUtils with LakeSoulSQLCommandTest {

  val dataset = "fashion-mnist"
  // val dataset = "glove-50-angular"
  // val dataset = "nytimes-256-angular"
  //  val dataset = "coco-i2i-512-angular"
  var numHashes = 256
  var signatureLength = 8
  var bucketLimit = 100000
  var bucketWidth = 16
  var filePath = "/Users/ceng/Downloads/ann-data/glove-50-angular.hdf5"
  var distanceType = "l2"

  var normType = "NORM_NONE"
  var minMaxLowerLimit = 0.0
  var minMaxUpperLimit = 1.0
  var zScoreTargetMean = 0.0
  var zScoreTargetStdDev = 1.0


  val seed = 1234567890
  if (dataset == "fashion-mnist") {
    filePath = "/Users/ceng/Downloads/ann-data/fashion-mnist-784-euclidean.hdf5"
    numHashes = 256
    signatureLength = 8
    bucketLimit = 2000
    bucketWidth = 20
    normType = "NORM_MINMAX"
    minMaxLowerLimit = 0.0
    minMaxUpperLimit = 1.0
    distanceType = "l2"
  } else if (dataset == "glove-50-angular") {
    filePath = "/Users/ceng/Downloads/ann-data/glove-50-angular.hdf5"
    numHashes = 256
    signatureLength = 8
    bucketLimit = 100000
    bucketWidth = 16
    normType = "NORM_NONE"
    distanceType = "cosine"
  } else if (dataset == "nytimes-256-angular") {
    filePath = "/Users/ceng/Downloads/ann-data/nytimes-256-angular.hdf5"
    numHashes = 128
    signatureLength = 4
    bucketLimit = 20000
    bucketWidth = 16
    normType = "NORM_NONE"
    minMaxLowerLimit = -1.0
    minMaxUpperLimit = 1.0
    distanceType = "cosine"
  } else if (dataset == "coco-i2i-512-angular") {
    filePath = "/Users/ceng/Downloads/ann-data/coco-i2i-512-angular.hdf5"
    numHashes = 128
    signatureLength = 4
    bucketLimit = 10000
    bucketWidth = 16
    normType = "NORM_NONE"
    distanceType = "cosine"
  }


  val testDatasetSize = 10000
  val numQuery = 10
  val topK = 100
  //  val sampleIndices = scala.util.Random.shuffle((0 until testDatasetSize).toList).take(numQuery)
  val sampleIndices = (0 until testDatasetSize).take(numQuery)
  //  val filePath = "/Users/ceng/Downloads/ann-data/fashion-mnist-784-euclidean.hdf5"
  val bucketNumOfBucketId = 8


  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)

    // Basic configuration
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
        val trainPath = trainDir.getCanonicalPath
        val testPath = testDir.getCanonicalPath

        try {
          val hdfFile = new HdfFile(Paths.get(filePath))

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

            // Calculate dot product
            val dotProduct = trainEmbedding.zip(testEmbedding).map { case (a, b) => a * b }.sum

            // Calculate L2 norms
            val normA = math.sqrt(trainEmbedding.map(x => x * x).sum)
            val normB = math.sqrt(testEmbedding.map(x => x * x).sum)

            // Return 1.0 minus the absolute cosine similarity
            1.0 - (math.abs(dotProduct) / (normA * normB))
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
              .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_EMBEDDING_DIMENSION, trainData.asInstanceOf[Array[Array[Float]]](0).length.toString)
              .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_BIT_WIDTH, "256")
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
              println(s"================ Saving train data to LakeSoul table: trainData ==================")
              spark.time({
                val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
                df.write.format("lakesoul")
                  .option("hashPartitions", "IndexId")
                  .option("hashBucketNum", 4)
                  .option(LakeSoulOptions.SHORT_TABLE_NAME, "trainData")
                  .mode("Overwrite").save(trainPath)
              })

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

                  println(s"================ Querying with LakeSoul LSH ==================")
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
    println(s"================ Calculating Recall ==================")
    var totalRecall = 0.0
    for (i <- sampleIndices.indices) {
      val trueSet = trueNeighbors(sampleIndices(i)).take(k).toSet
      val predictedSet = predictedNeighbors
        .filter(_.getAs[Long](0) == sampleIndices(i))
        .map(_.getAs[Long](1).toInt)
        .take(k)
        .toSet

      val intersection = trueSet.intersect(predictedSet)

      println(s"sampleIndex: ${sampleIndices(i)}\n trueSet: $trueSet, predictedSet: $predictedSet \n intersection: $intersection")
      totalRecall += intersection.size.toDouble / k
    }
    val recall = totalRecall / numSamples
    println(s"================ Recall: ${recall * 100}% ==================")
    recall
  }

  test("test spark BucketedRandomProjectionLSH on MNIST") {
    val hdfFile = new HdfFile(Paths.get(filePath))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
    val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
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
    val hdfFile = new HdfFile(Paths.get(filePath))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
    val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
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
    val hdfFile = new HdfFile(Paths.get(filePath))

    val trainDataset = hdfFile.getDatasetByPath("train")
    val testDataset = hdfFile.getDatasetByPath("test")
    val neighborDataset = hdfFile.getDatasetByPath("neighbors")
    val trainData = trainDataset.getData()
    val testData = testDataset.getData()
    val neighborData = neighborDataset.getData()

    // 将数据转换为向量格式
    val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
    val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
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

  test("test LakeSoul training data by BucketedRandomProjectionLSH on MNIST") {
    withTempDir { dir =>

      val hdfFile = new HdfFile(Paths.get(filePath))

      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()

      // 将数据转换为向量格式
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = neighborData.asInstanceOf[Array[Array[Int]]]

      val trainDf = spark.createDataFrame(
        spark.sparkContext.parallelize(trainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, vec)
        })
      ).toDF("id", "features")
      trainDf.write.format("lakesoul")
        .option("hashPartitions", "id")
        .option("hashBucketNum", 4)
        .option(LakeSoulOptions.SHORT_TABLE_NAME, "trainData")
        .mode("Overwrite")
        .save(dir.getCanonicalPath)

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

      val scanDf = LakeSoulTable.forPath(dir.getCanonicalPath).toDF

      // Create a UDF to convert array to vector
      val arrayToVector = udf { array: Seq[Float] =>
        org.apache.spark.ml.linalg.Vectors.dense(array.map(_.toDouble).toArray)
      }

      // Convert the features column from array to vector
      val scanDfWithVectors = scanDf.withColumn("features", arrayToVector(col("features")))

      // Create BRP LSH model
      val brp = new BucketedRandomProjectionLSH()
        .setInputCol("features")
        .setOutputCol("hashes")
        .setBucketLength(784)
        .setNumHashTables(10)

      // Transform candidate data with LSH
      val model = brp.fit(scanDfWithVectors)
      val transformedCandidates = model.transform(scanDfWithVectors)
      transformedCandidates.persist()

      // Create a DataFrame with all sampled test vectors at once
      val predictedNeighbors = spark.time({
        val sampledTestDF = spark.createDataFrame(
          spark.sparkContext.parallelize(
            sampleIndices.map { idx =>
              (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
            }
          )
        ).toDF("indexIdTest", "features")

        // Transform the sampled test data with LSH for batch processing
        val transformedTestVectors = model.transform(sampledTestDF)

        // Use approxSimilarityJoin to find nearest neighbors for all test vectors at once
        val allResultsDF = model.approxSimilarityJoin(
          transformedTestVectors,
          transformedCandidates,
          5000.0 // Using a large threshold to ensure we get enough candidates
        )
          .select(
            col("datasetA.indexIdTest").as("indexIdTest"),
            col("datasetB.id").as("indexIdTrain"),
            col("distCol").as("distance")
          )

        // Import Window functions for ranking
        import org.apache.spark.sql.expressions.Window
        val windowSpec = Window.partitionBy("indexIdTest").orderBy("distance")

        // Apply ranking and filter to get top-K for each test vector
        val topKNeighborsDF = allResultsDF
          .withColumn("rank", org.apache.spark.sql.functions.row_number().over(windowSpec))
          .filter(col("rank") <= topK)
          .drop("rank")

        // Collect the final results
        topKNeighborsDF.collect()

      })

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighbors, topK, numQuery)
      println(s"BRP LSH Average recall@$topK = $avgRecall")

    }
  }

  test("test LakeSoul training data by LinkedIn LSH on MNIST") {
    withTempDir { dir =>

      val hdfFile = new HdfFile(Paths.get(filePath))

      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()

      // 将数据转换为向量格式
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = neighborData.asInstanceOf[Array[Array[Int]]]

      // Write train data to LakeSoul
      val trainDf = spark.createDataFrame(
        spark.sparkContext.parallelize(trainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, vec)
        })
      ).toDF("id", "features")

      trainDf.write.format("lakesoul")
        .option("hashPartitions", "id")
        .option("hashBucketNum", 4)
        .option(LakeSoulOptions.SHORT_TABLE_NAME, "trainData")
        .mode("Overwrite")
        .save(dir.getCanonicalPath)

      // Read data back from LakeSoul
      val scanDf = LakeSoulTable.forPath(dir.getCanonicalPath).toDF

      // Create a UDF to convert array to vector
      val arrayToVector = udf { array: Seq[Float] =>
        org.apache.spark.ml.linalg.Vectors.dense(array.map(_.toDouble).toArray)
      }

      // Convert the features column from array to vector
      val scanDfWithVectors = scanDf.withColumn("features", arrayToVector(col("features")))

      // Prepare candidate RDD from LakeSoul data
      val candidateRDD = scanDfWithVectors.rdd.map { row =>
        (row.getAs[Long]("id"), row.getAs[org.apache.spark.ml.linalg.Vector]("features"))
      }

      // Prepare test data
      val sampledTestRDD = spark.sparkContext.parallelize(
        sampleIndices.map { case idx =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
        }
      )

      val numFeatures = trainVectors(0).length

      // Create LinkedIn LSH model
      val model = new L2ScalarRandomProjectionNNS()
        .setNumHashes(512)
        .setSignatureLength(16)
        .setBucketWidth(trainVectors.length / 8)
        .createModel(numFeatures)

      // Run similarity search and time it
      val result: Array[(Long, Long, Double)] = spark.time(
        model.getAllNearestNeighbors(sampledTestRDD, candidateRDD, topK).collect()
      )

      // Convert the results to Row format for recall calculation
      val predictedNeighborsAsRows = result.map { case (queryId, neighborId, distance) =>
        Row(queryId, neighborId, distance)
      }

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
      println(s"LinkedIn LSH with LakeSoul Storage Average recall@$topK = $avgRecall")
    }
  }

  test("test LakeSoul non-persistent LSH Index") {

    println(s"\n==== Running test with $normType ====\n")

    withTempDir { dir =>
      val hdfFile = new HdfFile(Paths.get(filePath))

      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()

      // Convert data to vectors
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = if (dataset == "coco-i2i-512-angular") {
        neighborData.asInstanceOf[Array[Array[Long]]].map(_.map(_.toInt))
      } else {
        neighborData.asInstanceOf[Array[Array[Int]]]
      }


      // Compute normalization parameters using DataNormalizer
      val normParams = DataNormalizer.computeNormalizationParams(
        trainVectors,
        normType,
        minMaxLower = minMaxLowerLimit,
        minMaxUpper = minMaxUpperLimit,
        zScoreTargetMean = zScoreTargetMean,
        zScoreTargetStdDev = zScoreTargetStdDev
      )

      // Apply normalization to training vectors
      val finalTrainVectors = DataNormalizer.normalizeData(trainVectors, normParams)

      // Write train data to LakeSoul (normalized or original)
      val trainDf = spark.createDataFrame(
        spark.sparkContext.parallelize(finalTrainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, vec)
        })
      ).toDF("id", "features")

      val tableName = normType match {
        case "NORM_NONE" => "trainDataOriginal"
        case "NORM_MINMAX" => "trainDataMinMax"
        case "NORM_ZSCORE" => "trainDataZScore"
      }

      println(s"================ Saving train data to LakeSoul table: $tableName ==================")
      spark.time({
        trainDf.write.format("lakesoul")
          .option("hashPartitions", "id")
          .option("hashBucketNum", 4)
          .option(LakeSoulOptions.SHORT_TABLE_NAME, tableName)
          .mode("Overwrite")
          .save(dir.getCanonicalPath)
      })

      // Read data back from LakeSoul
      val scanDf = LakeSoulTable.forPath(dir.getCanonicalPath).toDF

      // Create a UDF to convert array to vector
      val arrayToVector = udf { array: Seq[Double] =>
        org.apache.spark.ml.linalg.Vectors.dense(array.toArray)
      }

      // Convert the features column from array to vector
      val scanDfWithVectors = scanDf.withColumn("features", arrayToVector(col("features")))

      // Prepare candidate RDD from LakeSoul data
      val candidateRDD = scanDfWithVectors.rdd.map { row =>
        (row.getAs[Long]("id"), row.getAs[org.apache.spark.ml.linalg.Vector]("features"))
      }

      // Apply normalization to sampled test vectors
      val sampledTestVectors = sampleIndices.map(idx => testVectors(idx))
      val normalizedTestVectors = DataNormalizer.normalizeData(sampledTestVectors.toArray, normParams)

      // Prepare test data RDD
      val sampledTestRDD = spark.sparkContext.parallelize(
        sampleIndices.zip(normalizedTestVectors).map { case (idx, vec) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
        }
      )

      val numFeatures = trainVectors(0).length
      // Adjust bucket width based on normalization method


      // Create LakeSoulRandomProjectionNNS model
      val model = new LakeSoulRandomProjectionNNS()
        .setShouldSampleBuckets(true)
        .setSeed(seed)
        .setNumHashes(numHashes)
        .setSignatureLength(signatureLength)
        .setBucketWidth(bucketWidth)
        .setBucketLimit(bucketLimit)
        .setDistanceMetric(distanceType)
        .createModel(numFeatures)

      // Run similarity search and time it
      val result: Array[(Long, Long, Double)] = spark.time({
        val res = model.getAllNearestNeighbors(sampledTestRDD, candidateRDD, topK).collect()
        println(s"res = ${res.take(5).mkString("; ")}...")
        res
      })

      // Convert the results to Row format for recall calculation
      val predictedNeighborsAsRows = result.map { case (queryId, neighborId, distance) =>
        Row(queryId, neighborId, distance)
      }

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
      println(s"LakeSoul LSH with $normType: Average recall@$topK = $avgRecall")
    }


    // After running all tests, print a summary message
    println("\n==== Normalization Comparison Complete ====")
    println("Compare the recall results above to determine the best normalization strategy")
  }

  test("test LakeSoul persistent LSH Index") {
    withTempDir { dir =>
      val tableName = "test_lsh_index"

      val hdfFile = new HdfFile(Paths.get(filePath))
      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()


      // Convert data to vectors
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = if (dataset == "coco-i2i-512-angular") {
        neighborData.asInstanceOf[Array[Array[Long]]].map(_.map(_.toInt))
      } else {
        neighborData.asInstanceOf[Array[Array[Int]]]
      }

      val numFeatures = trainVectors(0).length

      // Use DataNormalizer to compute min-max normalization parameters
      val normParams = DataNormalizer.computeNormalizationParams(
        trainVectors,
        normType
      )

      // Apply min-max normalization to training vectors
      val normalizedTrainVectors = DataNormalizer.normalizeData(trainVectors, normParams)

      // Convert training vectors to RDD format
      val trainRDD = spark.sparkContext.parallelize(
        normalizedTrainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
        }
      )

      // Create a model using LakeSoulRandomProjectionNNS
      val model = new LakeSoulRandomProjectionNNS()
        .setJoinParallelism(500)
        .setSeed(seed)
        .setShouldSampleBuckets(true)
        .setNumHashes(numHashes)
        .setSignatureLength(signatureLength)
        .setBucketWidth(bucketWidth)
        .setBucketLimit(bucketLimit)
        .setDistanceMetric(distanceType)
        .createModel(numFeatures)

      // Transform the data to get hashed values
      val transformedData = model.transform(trainRDD)

      // Explode the data into bucket-to-vector mappings
      val explodedData = model.explodeData(transformedData)

      // Convert the exploded RDD to a DataFrame suitable for storage
      val explodedDF = explodedData.map { case ((bucketId, hashIndex, _), (vectorId, vector)) =>
        // Get the raw float array from the Vector
        val doubleArray = vector.toArray
        (bucketId, hashIndex, vectorId, doubleArray)
      }.toDF("bucket_id", "hash_index", "vector_id", "vector")

      // Debug: Print the schema of explodedDF
      println("Schema of explodedDF before saving:")
      explodedDF.printSchema()

      println("================ Saving LSH Index ==================")
      spark.time({
        // Save as LakeSoul table
        explodedDF.write.format("lakesoul")
          .option("rangePartitions", "hash_index, bucketed_bucket_id")
          .option(LakeSoulOptions.SHORT_TABLE_NAME, tableName)
          .mode("Overwrite")
          .save(dir.getCanonicalPath)
      })

      println(s"LSH Index saved as LakeSoul table at ${dir.getCanonicalPath}")

      // Read back the data to verify
      val readBackDF = LakeSoulTable.forPath(dir.getCanonicalPath).toDF

      // Normalize test vectors for query
      val sampledTestVectors = sampleIndices.map(idx => testVectors(idx))
      val normalizedTestVectors = DataNormalizer.normalizeData(sampledTestVectors.toArray, normParams)

      // Create RDD of normalized test vectors
      val sampledTestRDD = spark.sparkContext.parallelize(
        sampleIndices.zip(normalizedTestVectors).map { case (idx, vec) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec))
        }
      )

      val bias = 0
      println("================ Querying with LSH Index ==================")
      val batchSize = 25 // Process batchSize queries at a time
      val results = spark.time({
        // Split sampledTestRDD into batches
        val batches = sampledTestRDD.collect().grouped(batchSize).toSeq

        // Process each batch and collect results
        batches.flatMap { batchData =>
          val batchRDD = spark.sparkContext.parallelize(batchData)
          println(s"Processing batch of ${batchData.length} queries...")
          model.getAllNearestNeighborsWithIndex(batchRDD, readBackDF, topK, bias).collect()
        }.toArray
      })

      println(s"result = ${results.take(5).mkString("; ")}...")

      // Convert the results to Row format for recall calculation
      val predictedNeighborsAsRows = results.map { case (queryId, neighborId, distance) =>
        Row(queryId, neighborId, distance)
      }

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
      println(s"LakeSoul LSH : Average recall@$topK = $avgRecall")
    }
  }

  test("test LakeSoul persistent LSH Partitioned Index") {
    withTempDir { dir =>
      val tableName = "test_lsh_index"

      val hdfFile = new HdfFile(Paths.get(filePath))
      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()


      // Convert data to vectors
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = if (dataset == "coco-i2i-512-angular") {
        neighborData.asInstanceOf[Array[Array[Long]]].map(_.map(_.toInt))
      } else {
        neighborData.asInstanceOf[Array[Array[Int]]]
      }

      val numFeatures = trainVectors(0).length

      // Use DataNormalizer to compute min-max normalization parameters
      val normParams = DataNormalizer.computeNormalizationParams(
        trainVectors,
        normType
      )

      // Apply min-max normalization to training vectors
      val normalizedTrainVectors = DataNormalizer.normalizeData(trainVectors, normParams)

      // Convert training vectors to RDD format
      val trainRDD = spark.sparkContext.parallelize(
        normalizedTrainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
        }
      )

      // Create a model using LakeSoulRandomProjectionNNS
      val model = new LakeSoulRandomProjectionNNS()
        .setJoinParallelism(500)
        .setSeed(seed)
        .setShouldSampleBuckets(true)
        .setNumHashes(numHashes)
        .setSignatureLength(signatureLength)
        .setBucketWidth(bucketWidth)
        .setBucketLimit(bucketLimit)
        .setDistanceMetric(distanceType)
        .createModel(numFeatures)

      // Transform the data to get hashed values
      val transformedData = model.transform(trainRDD)

      // Explode the data into bucket-to-vector mappings
      val explodedData = model.explodeData(transformedData, mod = bucketNumOfBucketId)

      // Convert the exploded RDD to a DataFrame suitable for storage
      val explodedDF = explodedData.map { case ((bucketId, hashIndex, bucketedBucketId), (vectorId, vector)) =>
        // Get the raw float array from the Vector
        val doubleArray = vector.toArray
        (bucketId, hashIndex, bucketedBucketId, vectorId, doubleArray)
      }.toDF("bucket_id", "hash_index", "bucketed_bucket_id", "vector_id", "vector")
        .sort("bucket_id")

      // Debug: Print the schema of explodedDF
      println("Schema of explodedDF before saving:")
      explodedDF.printSchema()

      println("================ Saving LSH Index ==================")
      spark.time({
        // Save as LakeSoul table
        explodedDF.write.format("lakesoul")
          .option("rangePartitions", "hash_index, bucketed_bucket_id")
          .option(LakeSoulOptions.SHORT_TABLE_NAME, tableName)
          .mode("Overwrite")
          .save(dir.getCanonicalPath)
      })

      println(s"LSH Index saved as LakeSoul table at ${dir.getCanonicalPath}")

      // Read back the data to verify
      val readBackDF = LakeSoulTable.forPath(dir.getCanonicalPath).toDF

      // Normalize test vectors for query
      val sampledTestVectors = sampleIndices.map(idx => testVectors(idx))
      val normalizedTestVectors = DataNormalizer.normalizeData(sampledTestVectors.toArray, normParams)

      // Create RDD of normalized test vectors
      val sampledTestRDD = spark.sparkContext.parallelize(
        sampleIndices.zip(normalizedTestVectors).map { case (idx, vec) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec))
        }
      )

      val bias = 0
      println("================ Querying with LSH Index ==================")
      val batchSize = 50 // Process batchSize queries at a time
      val results = spark.time({
        // Split sampledTestRDD into batches
        val batches = sampledTestRDD.collect().grouped(batchSize).toSeq

        // Process each batch and collect results
        batches.flatMap { batchData =>
          val batchRDD = spark.sparkContext.parallelize(batchData)
          println(s"Processing batch of ${batchData.length} queries...")
          model.getAllNearestNeighborsWithIndex(batchRDD, readBackDF, topK, bias, mod = bucketNumOfBucketId).collect()
        }.toArray
      })

      println(s"result = ${results.take(5).mkString("; ")}...")

      // Convert the results to Row format for recall calculation
      val predictedNeighborsAsRows = results.map { case (queryId, neighborId, distance) =>
        Row(queryId, neighborId, distance)
      }

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
      println(s"LakeSoul LSH : Average recall@$topK = $avgRecall")
    }
  }

  test("test LakeSoul persistent LSH Partitioned Index With SQL Query") {
    withTempDir { dir =>
      val tableName = "test_lsh_index"

      val hdfFile = new HdfFile(Paths.get(filePath))
      val trainDataset = hdfFile.getDatasetByPath("train")
      val testDataset = hdfFile.getDatasetByPath("test")
      val neighborDataset = hdfFile.getDatasetByPath("neighbors")
      val trainData = trainDataset.getData()
      val testData = testDataset.getData()
      val neighborData = neighborDataset.getData()

      // Convert data to vectors
      val trainVectors = trainData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val testVectors = testData.asInstanceOf[Array[Array[Float]]].map(_.map(_.toDouble))
      val trueNeighbors = if (dataset == "coco-i2i-512-angular") {
        neighborData.asInstanceOf[Array[Array[Long]]].map(_.map(_.toInt))
      } else {
        neighborData.asInstanceOf[Array[Array[Int]]]
      }

      val numFeatures = trainVectors(0).length

      // Use DataNormalizer to compute min-max normalization parameters
      val normParams = DataNormalizer.computeNormalizationParams(
        trainVectors,
        normType
      )

      // Apply min-max normalization to training vectors
      val normalizedTrainVectors = DataNormalizer.normalizeData(trainVectors, normParams)

      // Convert training vectors to RDD format
      val trainRDD = spark.sparkContext.parallelize(
        normalizedTrainVectors.zipWithIndex.map { case (vec, idx) =>
          (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(vec.map(_.toDouble)))
        }
      )

      // Create a model using LakeSoulRandomProjectionNNS
      val model = new LakeSoulRandomProjectionNNS()
        .setJoinParallelism(500)
        .setSeed(seed)
        .setShouldSampleBuckets(true)
        .setNumHashes(numHashes)
        .setSignatureLength(signatureLength)
        .setBucketWidth(bucketWidth)
        .setBucketLimit(bucketLimit)
        .setDistanceMetric(distanceType)
        .createModel(numFeatures)

      // Transform the data to get hashed values
      val transformedData = model.transform(trainRDD)

      // Explode the data into bucket-to-vector mappings
      val explodedData = model.explodeData(transformedData, mod = bucketNumOfBucketId)

      // Convert the exploded RDD to a DataFrame suitable for storage
      val explodedDF = explodedData.map { case ((bucketId, hashIndex, bucketedBucketId), (vectorId, vector)) =>
        // Get the raw float array from the Vector
        val doubleArray = vector.toArray
        (bucketId, hashIndex, bucketedBucketId, vectorId, doubleArray)
      }.toDF("bucket_id", "hash_index", "bucketed_bucket_id", "vector_id", "vector")
        .sort("bucket_id")

      // Debug: Print the schema of explodedDF
      println("Schema of explodedDF before saving:")
      explodedDF.printSchema()

      println("================ Saving LSH Index ==================")
      spark.time({
        // Save as LakeSoul table
        explodedDF.write.format("lakesoul")
          .option("rangePartitions", "hash_index, bucketed_bucket_id")
          .option(LakeSoulOptions.SHORT_TABLE_NAME, tableName)
          .mode("Overwrite")
          .save(dir.getCanonicalPath)
      })

      println(s"LSH Index saved as LakeSoul table at ${dir.getCanonicalPath}")

      // Normalize test vectors for query
      val sampledTestVectors = sampleIndices.map(idx => testVectors(idx))
      val normalizedTestVectors = DataNormalizer.normalizeData(sampledTestVectors.toArray, normParams)

      // Create a DataFrame from normalized test vectors for visualization and debugging
      val normalizedTestDF = spark.createDataFrame(
        sampleIndices.zip(normalizedTestVectors).map { case (idx, vec) =>
          (idx.toLong, vec.toSeq)
        }
      ).toDF("id", "normalized_vector")

      // Create a temporary view for SQL queries
      normalizedTestDF.createOrReplaceTempView("normalized_test_vectors")

      // Register the LSH UDFs
      spark.registerFunction()

      // Get LSH parameters for queries
      val queryParams = Array(numHashes, signatureLength, bucketWidth, seed.toLong)

      println("================ Querying with LSH Index using SQL ==================")

      // Record the query execution time
      val sqlResults = spark.time({
        // Directly create test_bucket_components with a single SQL query
        val bucketComponents = spark.sql(
          s"""
             |SELECT
             |  ntv.id as query_id,
             |  ntv.normalized_vector,
             |  bucket_info._1 as bucket_id,
             |  bucket_info._2 as hash_index,
             |  bucket_info._1 % ${bucketNumOfBucketId} as bucketed_bucket_id
             |FROM normalized_test_vectors ntv
             |LATERAL VIEW explode(computeLSHBuckets(ntv.normalized_vector, ${queryParams(0)}, ${queryParams(1)}, ${queryParams(2)}, ${queryParams(3)})) exploded AS bucket_info
          """.stripMargin)

        bucketComponents.createOrReplaceTempView("test_bucket_components")

        // 1. First collect the distinct filter keys
        val filterKeys = spark.sql(
          """
          SELECT DISTINCT hash_index, bucketed_bucket_id 
          FROM test_bucket_components
          """
        ).collect()

        // 2. Build the WHERE condition with literal values
        val filterCondition = filterKeys.map { row =>
          s"(lsh.hash_index = ${row.getInt(0)} AND lsh.bucketed_bucket_id = ${row.getInt(1)})"
        }.mkString(" OR ")
        
        // 3. Execute the filtered query with literal values
        val filteredLSH = spark.sql(
          s"""
             |SELECT lsh.*
             |FROM $tableName lsh
             |WHERE $filterCondition
          """.stripMargin)

        filteredLSH.createOrReplaceTempView("filtered_lsh")

        // Calculate distances and find top-K nearest neighbors with a simplified query for single-query case
        val distanceTypeSql = if (distanceType == "l2") "computeL2Distance" else "computeCosineDistance"

        val topKNeighbors = if (numQuery == 1) {
          // Simplified merged query for single query case with DISTINCT
          spark.sql(
            s"""
               |SELECT DISTINCT
               |  tbc.query_id,
               |  filtered_lsh.vector_id,
               |  ${distanceTypeSql}(tbc.normalized_vector, filtered_lsh.vector) as distance
               |FROM test_bucket_components tbc
               |JOIN filtered_lsh
               |  ON tbc.bucket_id = filtered_lsh.bucket_id
               |  AND tbc.hash_index = filtered_lsh.hash_index
               |ORDER BY distance
               |LIMIT $topK
            """.stripMargin)
        } else {
          // Merged query with window function for multiple queries
          spark.sql(
            s"""
               |WITH distinct_pairs AS (
               |  SELECT DISTINCT
               |    tbc.query_id,
               |    filtered_lsh.vector_id,
               |    ${distanceTypeSql}(tbc.normalized_vector, filtered_lsh.vector) as distance
               |  FROM test_bucket_components tbc
               |  JOIN filtered_lsh
               |    ON tbc.bucket_id = filtered_lsh.bucket_id
               |    AND tbc.hash_index = filtered_lsh.hash_index
               |),
               |ranked_neighbors AS (
               |  SELECT
               |    query_id,
               |    vector_id,
               |    distance,
               |    ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY distance) as rank
               |  FROM distinct_pairs
               |)
               |SELECT query_id, vector_id, distance
               |FROM ranked_neighbors
               |WHERE rank <= $topK
               |ORDER BY query_id, distance
            """.stripMargin)
        }

        // After collecting SQL results
        topKNeighbors.collect()
      })

      // Convert the SQL results to the format needed for recall calculation
      val predictedNeighborsAsRows = sqlResults.map { row =>
        Row(row.getLong(0), row.getLong(1), row.getDouble(2))
      }

      // Calculate and print recall
      val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery)
      println(s"LakeSoul LSH SQL Query: Average recall@$topK = $avgRecall")
    }
  }

  implicit class SparkSessionExtensions(spark: SparkSession) {
    def registerFunction(): Unit = {

      // UDF to compute bucket IDs from a vector using LSH
      // This mimics model.getBandedHashes and getMur3HashCode functionality
      spark.udf.register("computeLSHBuckets", (inputVector: Seq[Double], numHashes: Int, signatureLength: Int, bucketWidth: Double, seed: Long) => {
        import scala.util.hashing.MurmurHash3
        import org.apache.spark.ml.lakesoul.scanns.linalg.RandomProjection
        import org.apache.spark.ml.lakesoul.scanns.lsh.ScalarRandomProjectionHashFunction
        import org.apache.spark.ml.linalg.Vectors

        val vector = Vectors.dense(inputVector.toArray)
        val rand = new java.util.Random(seed)

        // Create hash functions similar to what the model would do
        val randomProjections = Array.fill(numHashes / signatureLength) {
          new ScalarRandomProjectionHashFunction(
            RandomProjection.generateGaussian(inputVector.length, signatureLength, rand),
            bucketWidth
          )
        }

        // Calculate hashes for the vector
        val hashes = randomProjections.flatMap(_.compute(vector))

        // Group hashes into bands
        val bandedHashes = hashes.grouped(signatureLength).toArray

        // Calculate bucket IDs using MurmurHash3
        bandedHashes.zipWithIndex.map { case (hash, index) =>
          (MurmurHash3.arrayHash(hash), index, hash.mkString(","))
        }
      })

      // UDF to compute L2 distance between vectors
      spark.udf.register("computeL2Distance", (vec1: Seq[Double], vec2: Seq[Double]) => {
        require(vec1.length == vec2.length, "Vectors must have same dimension")
        math.sqrt(vec1.zip(vec2).map { case (a, b) => math.pow(a - b, 2) }.sum)
      })

      // UDF to compute cosine distance between vectors
      spark.udf.register("computeCosineDistance", (vec1: Seq[Double], vec2: Seq[Double]) => {
        require(vec1.length == vec2.length, "Vectors must have same dimension")

        val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum
        val normA = math.sqrt(vec1.map(x => x * x).sum)
        val normB = math.sqrt(vec2.map(x => x * x).sum)

        1.0 - (math.abs(dotProduct) / (normA * normB))
      })


    }
  }

}
