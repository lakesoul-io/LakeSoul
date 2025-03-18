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
import org.apache.spark.sql.functions.{collect_list, sum, udf, col, lit}
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
  //  val sampleIndices = scala.util.Random.shuffle((0 until testDatasetSize).toList).take(numQuery)
  val sampleIndices = (0 until testDatasetSize).take(numQuery)

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
        val filepath = "/Users/ceng/Downloads/fashion-mnist-784-euclidean.hdf5"
        val trainPath = trainDir.getCanonicalPath
        val testPath = testDir.getCanonicalPath

        println(filepath)

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
                               numSamples: Int,
                               bias: Int = 0): Double = {
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

      val intersection = trueSet.intersect(predictedSet)
      println(intersection)
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

  test("test LakeSoul training data by BucketedRandomProjectionLSH on MNIST") {
    withTempDir { dir =>

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

  test("test LakeSoul training data by LakeSoul LSH on MNIST") {
    // Define normalization types
    val NORM_NONE = 0
    val NORM_MINMAX = 1
    val NORM_ZSCORE = 2

    // Z-score normalization parameters
    val zScoreTargetMean = 0
    val zScoreTargetStdDev = 1.0

    // Configure which normalization methods to test
    // Options: NORM_NONE, NORM_MINMAX, NORM_ZSCORE
    val normalizationTypes = Seq(
      //      NORM_ZSCORE,
      NORM_MINMAX,
      //      NORM_NONE
    )

    normalizationTypes.foreach { normType =>
      val normName = normType match {
        case NORM_NONE => "No Normalization"
        case NORM_MINMAX => "Min-Max Normalization"
        case NORM_ZSCORE => s"Z-Score Normalization(mean=$zScoreTargetMean, stdDev=$zScoreTargetStdDev)"
      }

      println(s"\n==== Running test with $normName ====\n")

      withTempDir { dir =>
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

        // Memory variables for normalization parameters
        var broadcastFeatureMeans: org.apache.spark.broadcast.Broadcast[Array[Double]] = null
        var broadcastFeatureStdDevs: org.apache.spark.broadcast.Broadcast[Array[Double]] = null
        var broadcastFeatureMin: org.apache.spark.broadcast.Broadcast[Array[Float]] = null
        var broadcastFeatureMax: org.apache.spark.broadcast.Broadcast[Array[Float]] = null

        // Store original vectors for the non-normalized version
        val finalTrainVectors = normType match {
          case NORM_NONE =>
            // Use original vectors without normalization
            println("Using original vectors without normalization")
            trainVectors

          case NORM_MINMAX =>
            // Calculate global min-max normalization parameters
            println("Calculating global min-max normalization parameters")
            val numFeatures = trainVectors(0).length
            val featureMin = Array.fill(numFeatures)(Float.MaxValue)
            val featureMax = Array.fill(numFeatures)(Float.MinValue)

            // Find min and max for each feature across all training vectors
            trainVectors.foreach { vec =>
              for (i <- 0 until numFeatures) {
                featureMin(i) = math.min(featureMin(i), vec(i))
                featureMax(i) = math.max(featureMax(i), vec(i))
              }
            }

            // Broadcast min and max values to make them available to all executors
            broadcastFeatureMin = spark.sparkContext.broadcast(featureMin)
            broadcastFeatureMax = spark.sparkContext.broadcast(featureMax)

            println(s"Min-max parameters calculated and broadcast to executors")
            println(s"Sample min values (first 5): ${featureMin.take(5).mkString(", ")}")
            println(s"Sample max values (first 5): ${featureMax.take(5).mkString(", ")}")

            // Create normalized versions of train vectors using min-max scaling
            val normalizedTrainVectors = trainVectors.map { vec =>
              vec.zipWithIndex.map { case (value, i) =>
                if (featureMax(i) - featureMin(i) > 0.0001f) {
                  (value - featureMin(i)) / (featureMax(i) - featureMin(i))
                } else {
                  0.0f // Handle case where max = min (constant feature)
                }
              }
            }

            normalizedTrainVectors

          case NORM_ZSCORE =>
            // Calculate global normalization parameters for custom normal distribution
            println(s"Calculating normalization parameters for norm($zScoreTargetMean, $zScoreTargetStdDev)")
            val numFeatures = trainVectors(0).length
            val numVectors = trainVectors.length

            // Step 1: Calculate means for each dimension
            val means = Array.fill(numFeatures)(0.0)
            trainVectors.foreach { vec =>
              for (i <- 0 until numFeatures) {
                means(i) += vec(i)
              }
            }

            // Divide sums by count to get means
            for (i <- 0 until numFeatures) {
              means(i) /= numVectors
            }

            // Step 2: Calculate variances for each dimension
            val variances = Array.fill(numFeatures)(0.0)
            trainVectors.foreach { vec =>
              for (i <- 0 until numFeatures) {
                val diff = vec(i) - means(i)
                variances(i) += diff * diff
              }
            }

            // Divide sum of squared differences by count to get variances
            for (i <- 0 until numFeatures) {
              variances(i) /= numVectors
            }

            // Step 3: Calculate standard deviations
            val stdDevs = variances.map(variance => math.sqrt(variance))

            // Broadcast means and stdDevs values to make them available to all executors
            broadcastFeatureMeans = spark.sparkContext.broadcast(means)
            broadcastFeatureStdDevs = spark.sparkContext.broadcast(stdDevs)

            println(s"Normalization parameters calculated and broadcast to executors")
            println(s"Sample means (first 5): ${means.take(5).mkString(", ")}")
            println(s"Sample stdDevs (first 5): ${stdDevs.take(5).mkString(", ")}")

            // Create normalized versions of train vectors with target mean and stdDev
            val normalizedTrainVectors = trainVectors.map { vec =>
              vec.zipWithIndex.map { case (value, i) =>
                if (stdDevs(i) > 0.0001) {
                  // First convert to standard z-score (mean=0, stdDev=1), 
                  // then scale to target stdDev and shift to target mean
                  ((value - means(i)) / stdDevs(i) * zScoreTargetStdDev + zScoreTargetMean).toFloat
                } else {
                  zScoreTargetMean.toFloat // Handle case where stdDev is near zero (constant feature)
                }
              }
            }

            normalizedTrainVectors
        }

        // Write train data to LakeSoul (either normalized or original)
        val trainDf = spark.createDataFrame(
          spark.sparkContext.parallelize(finalTrainVectors.zipWithIndex.map { case (vec, idx) =>
            (idx.toLong, vec)
          })
        ).toDF("id", "features")

        val tableName = normType match {
          case NORM_NONE => "trainDataOriginal"
          case NORM_MINMAX => "trainDataMinMax"
          case NORM_ZSCORE => "trainDataZScore"
        }

        trainDf.write.format("lakesoul")
          .option("hashPartitions", "id")
          .option("hashBucketNum", 4)
          .option(LakeSoulOptions.SHORT_TABLE_NAME, tableName)
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

        // Prepare test data (either normalized or original)
        val sampledTestRDD = normType match {
          case NORM_NONE =>
            // Use original test vectors
            spark.sparkContext.parallelize(
              sampleIndices.map { case idx =>
                (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(testVectors(idx).map(_.toDouble)))
              }
            )

          case NORM_MINMAX =>
            // Use the broadcast variables of min-max normalization parameters
            spark.sparkContext.parallelize(
              sampleIndices.map { case idx =>
                // Normalize the test vector using broadcast min/max values directly
                val testVec = testVectors(idx)
                val normalizedTestVec = testVec.zipWithIndex.map { case (value, i) =>
                  val min = broadcastFeatureMin.value(i)
                  val max = broadcastFeatureMax.value(i)
                  if (max - min > 0.0001f) {
                    (value - min) / (max - min)
                  } else {
                    0.0f
                  }
                }
                (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(normalizedTestVec.map(_.toDouble)))
              }
            )

          case NORM_ZSCORE =>
            // Use the broadcast variables of z-score normalization parameters
            spark.sparkContext.parallelize(
              sampleIndices.map { case idx =>
                // Normalize the test vector using broadcast means/stdDevs values directly
                val testVec = testVectors(idx)
                val normalizedTestVec = testVec.zipWithIndex.map { case (value, i) =>
                  val mean = broadcastFeatureMeans.value(i)
                  val stdDev = broadcastFeatureStdDevs.value(i)
                  if (stdDev > 0.0001) {
                    // First convert to standard z-score (mean=0, stdDev=1), 
                    // then scale to target stdDev and shift to target mean
                    ((value - mean) / stdDev * zScoreTargetStdDev + zScoreTargetMean).toFloat
                  } else {
                    zScoreTargetMean.toFloat
                  }
                }
                (idx.toLong, org.apache.spark.ml.linalg.Vectors.dense(normalizedTestVec.map(_.toDouble)))
              }
            )
        }

        val numFeatures = trainVectors(0).length
        // Adjust bucket width based on normalization method
        val bucketWidth = normType match {
          case NORM_NONE => trainVectors.length / 8
          case NORM_MINMAX => numFeatures / 32
          case NORM_ZSCORE => numFeatures / 4 * zScoreTargetStdDev.toFloat // Adjust based on target stdDev
        }

        // Create LakeSoulRandomProjectionNNS model
        val model = new LakeSoulRandomProjectionNNS()
          .setShouldSampleBuckets(true)
          .setSeed(1234)
          .setNumHashes(256)
          .setSignatureLength(8)
          .setBucketWidth(bucketWidth)
          .setBucketLimit(2000)
          //          .setJoinParallelism(50)
          //          .setNumOutputPartitions(50)
          .createModel(numFeatures)

        val bias = 20
        // Run similarity search and time it
        val result: Array[(Long, Long, Double)] = spark.time({
          val res = model.getAllNearestNeighborsWithBucketBias(sampledTestRDD, candidateRDD, topK, 0, bias).collect()
          println(s"res = ${res.take(5).mkString("; ")}...")
          res
        })

        // Convert the results to Row format for recall calculation
        val predictedNeighborsAsRows = result.map { case (queryId, neighborId, distance) =>
          Row(queryId, neighborId, distance)
        }

        // Calculate and print recall
        val avgRecall = calculateRecall(sampleIndices, trueNeighbors, predictedNeighborsAsRows, topK, numQuery, bias)
        println(s"LakeSoul LSH with $normName: Average recall@$topK = $avgRecall")

        // Print a sample vector normalization example
        if (normType != NORM_NONE) {
          // Demonstrate using the broadcast normalization parameters
          println("Demonstrating normalization using in-memory parameters")

          // Get a sample test vector
          val sampleTestVector = testVectors(0)

          // Apply the appropriate normalization
          val normalizedSampleTest = normType match {
            case NORM_MINMAX =>
              sampleTestVector.zipWithIndex.map { case (value, i) =>
                val min = broadcastFeatureMin.value(i)
                val max = broadcastFeatureMax.value(i)
                if (max - min > 0.0001f) {
                  (value - min) / (max - min)
                } else {
                  0.0f
                }
              }

            case NORM_ZSCORE =>
              sampleTestVector.zipWithIndex.map { case (value, i) =>
                val mean = broadcastFeatureMeans.value(i)
                val stdDev = broadcastFeatureStdDevs.value(i)
                if (stdDev > 0.0001) {
                  ((value - mean) / stdDev * zScoreTargetStdDev + zScoreTargetMean).toFloat
                } else {
                  zScoreTargetMean.toFloat
                }
              }

            case _ => sampleTestVector // This won't be reached due to the if condition
          }

          println(s"Original test vector first 5 values: ${sampleTestVector.take(5).mkString(", ")}")
          println(s"$normName vector first 5 values: ${normalizedSampleTest.take(5).mkString(", ")}")

          // For min-max normalization, verify values are in [0,1]
          if (normType == NORM_MINMAX) {
            val minNormalized = normalizedSampleTest.min
            val maxNormalized = normalizedSampleTest.max
            println(s"Min-max normalized range: [$minNormalized, $maxNormalized] (should be close to [0,1])")
          }

          // For z-score normalization, check mean and std
          if (normType == NORM_ZSCORE) {
            val meanNormalized = normalizedSampleTest.sum / normalizedSampleTest.length
            val varianceNormalized = normalizedSampleTest.map(x => math.pow(x - meanNormalized, 2)).sum / normalizedSampleTest.length
            val stdNormalized = math.sqrt(varianceNormalized)
            println(s"Z-score normalized stats: mean = $meanNormalized, std = $stdNormalized (should be close to $zScoreTargetMean and $zScoreTargetStdDev)")
          }
        }
      }
    }

    // After running all tests, print a summary message
    println("\n==== Normalization Comparison Complete ====")
    println("Compare the recall results above to determine the best normalization strategy")
  }
}
