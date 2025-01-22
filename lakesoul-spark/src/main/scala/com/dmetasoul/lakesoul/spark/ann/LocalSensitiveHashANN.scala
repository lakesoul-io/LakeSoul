package com.dmetasoul.lakesoul.spark.ann

import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import io.jhdf.HdfFile
import org.apache.spark.sql.functions.{collect_list, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import java.nio.file.Paths
import scala.concurrent.duration.DurationLong
import scala.math.{pow, sqrt}

object LocalSensitiveHashANN {

  case class LoadConfig(
    hdf5File: String = "",
    warehouse: String = "",
    tableName: String = "ann_table",
    embeddingDim: Int = 0,
    bitWidth: Int = 512,
  )

  case class QueryConfig(
    hdf5File: String = "",
    tableName: String = "",
    queryLimit: Int = 100,
    topk: Int = 100,
    nFactor: Int = 3
  )

  sealed trait Command
  case class Load(config: LoadConfig) extends Command
  case class Query(config: QueryConfig) extends Command

  def parseArgs(args: Array[String]): Option[Command] = {
    if (args.isEmpty) {
      printUsage()
      return None
    }

    args(0).toLowerCase match {
      case "load" => parseLoadCommand(args.tail)
      case "query" => parseQueryCommand(args.tail)
      case _ =>
        println(s"未知命令: ${args(0)}")
        printUsage()
        None
    }
  }

  private def parseLoadCommand(args: Array[String]): Option[Command] = {
    var config = LoadConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--hdf5-file" if i + 1 < args.length =>
          config = config.copy(hdf5File = args(i + 1))
          i += 2
        case "--warehouse" if i + 1 < args.length =>
          config = config.copy(warehouse = args(i + 1))
          i += 2
        case "--table-name" if i + 1 < args.length =>
          config = config.copy(tableName = args(i + 1))
          i += 2
        case "--embedding-dim" if i + 1 < args.length =>
          config = config.copy(embeddingDim = args(i + 1).toInt)
          i += 2
        case "--bit-width" if i + 1 < args.length =>
          config = config.copy(bitWidth = args(i + 1).toInt)
          i += 2
        case arg =>
          println(s"未知的load参数: $arg")
          printUsage()
          return None
      }
    }

    if (config.hdf5File.isEmpty || config.warehouse.isEmpty || config.embeddingDim <= 0) {
      println("缺少必需参数: --hdf5-file 和 --warehouse 和 --embedding-dim")
      printUsage()
      None
    } else {
      Some(Load(config))
    }
  }

  private def parseQueryCommand(args: Array[String]): Option[Command] = {
    var config = QueryConfig()
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "--hdf5-file" if i + 1 < args.length =>
          config = config.copy(hdf5File = args(i + 1))
          i += 2
        case "--table-name" if i + 1 < args.length =>
          config = config.copy(tableName = args(i + 1))
          i += 2
        case "--query-limit" if i + 1 < args.length =>
          config = config.copy(queryLimit = args(i + 1).toInt)
          i += 2
        case "--topk" if i + 1 < args.length =>
          config = config.copy(topk = args(i + 1).toInt)
          i += 2
        case "--n-factor" if i + 1 < args.length =>
          config = config.copy(nFactor = args(i + 1).toInt)
          i += 2
        case arg =>
          println(s"未知的query参数: $arg")
          printUsage()
          return None
      }
    }

    if (config.tableName.isEmpty) {
      println("缺少必需参数: --table-name")
      printUsage()
      None
    } else {
      Some(Query(config))
    }
  }

  private def printUsage(): Unit = {
    println(
      """用法:
        |  load命令:
        |    load --hdf5-file <file> --warehouse <path> [--table-name <name>]
        |
        |  query命令:
        |    query --hdf5-file <file> --table-name <path> [--query-limit <num>] [--topk <num>] [--n-factor <num>]
        |
        |参数说明:
        |  load命令参数:
        |    --hdf5-file    : HDF5文件路径(必需)
        |    --warehouse    : LakeSoul仓库路径(必需)
        |    --table-name   : 表名(可选,默认:ann_table)
        |    --embedding-dim: 嵌入维度(必需,要求与HDF5文件中的embedding维度一致)
        |    --bit-width    : 位宽(可选,默认:512)
        |
        |  query命令参数:
        |    --hdf5-file    : HDF5文件路径(必需)
        |    --table-name   : 表名(必需)
        |    --query-limit  : 查询数据量(可选,默认:100)
        |    --topk         : 返回最近邻的数量(可选,默认:100)
        |    --n-factor     : LSH第一阶段过滤倍数(可选,默认:3)
        |""".stripMargin)
  }


  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("LocalSensitiveHashANN")
      .master("local[1]")
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)      

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val command = parseArgs(args)
    command match {
      case Some(Load(config)) => 
        val hdfFile = new HdfFile(Paths.get(config.hdf5File))

        val trainDataset = hdfFile.getDatasetByPath("train")
        val testDataset = hdfFile.getDatasetByPath("test")
        val trainData = trainDataset.getData()
        val testData = testDataset.getData()

        val schema = StructType(Array(
          StructField("IndexId", IntegerType, true),
          StructField("Embedding", ArrayType(FloatType), true, new MetadataBuilder()
            .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_EMBEDDING_DIMENSION, config.embeddingDim.toString)
            .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_BIT_WIDTH, config.bitWidth.toString)
            .putString(LakeSoulOptions.SchemaFieldMetadata.LSH_RNG_SEED, "1234567890").build()),
          StructField("Embedding_LSH", ArrayType(LongType), true),
          StructField("Split", StringType, true)
        ))
        trainData match {
          case float2DData: Array[Array[Float]] =>
            val classIds = (1 to float2DData.length).toArray

            val rows = float2DData.zip(classIds).map {
              case (embedding, indexId) =>
                //                  Row(indexId, embedding)
                Row(indexId, embedding, null, "train")
            }
            val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
            df.write.format("lakesoul")
              .option("hashPartitions", "IndexId")
              .option("hashBucketNum", 4)
              .option(LakeSoulOptions.SHORT_TABLE_NAME, config.tableName)
              .mode("Overwrite").save(s"${config.warehouse}/${config.tableName}")
          case _ =>
            println("Invalid train data")
        }


        testData match {
          case float2DTestData: Array[Array[Float]] =>
            val classIdsTest = (1 to float2DTestData.length).toArray

            val rowsTest = float2DTestData.zip(classIdsTest).map {
              case (embedding, indexId) =>
                //                      Row(indexId, embedding)
                Row(indexId, embedding, null, "test")
            }

            // val num = 50
            val dfTest = spark.createDataFrame(spark.sparkContext.parallelize(rowsTest), schema)
            LakeSoulTable.forPath(s"${config.warehouse}/${config.tableName}").upsert(dfTest)

          case _ =>
            println("Invalid test data")
        }
        

      case Some(Query(config)) => 
        val hdfFile = new HdfFile(Paths.get(config.hdf5File))
        val neighborDataset = hdfFile.getDatasetByPath("neighbors")
        val neighborData = neighborDataset.getData()
        var float2DDataNeighbor: Array[Array[Int]] = null
        neighborData match {
          case data: Array[Array[Int]] =>
            float2DDataNeighbor = data
          case _ =>
            println("invalid neighbor data")
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

        val startTime = System.nanoTime()
        val result = spark.sql(
          s"""
                  SELECT *
                  FROM (
                      SELECT
                          testData.IndexId AS indexIdTest,
                          trainData.IndexId AS indexIdTrain,
                          testData.Embedding as EmbeddingTest,
                          trainData.Embedding as EmbeddingTrain,
                          ROW_NUMBER() OVER (PARTITION BY testData.IndexId ORDER BY calculateHammingDistance(testData.Embedding_LSH, trainData.Embedding_LSH) asc) AS rank
                      FROM (SELECT * FROM ${config.tableName} WHERE Split = 'test' LIMIT ${config.queryLimit}) testData
                      CROSS JOIN (SELECT * FROM ${config.tableName} WHERE Split = 'train') trainData
                  ) ranked
                  WHERE rank <= ${config.nFactor * config.topk}
              """)
        result.createOrReplaceTempView("rank")
        val reResult = spark.sql(
          s"""
                SELECT *
                FROM (
                  SELECT
                    rank.indexIdTest,
                    rank.indexIDTrain,
                    ROW_NUMBER() OVER(PARTITION BY rank.indexIdTest ORDER BY calculateEuclideanDistance(rank.EmbeddingTest,rank.EmbeddingTrain) asc) AS reRank
                  FROM rank
                ) reRanked
                WHERE reRank <= ${config.topk}
                """).groupBy("indexIdTest").agg(collect_list("indexIdTrain").alias("indexIdTrainList"))


        val endTime = System.nanoTime()
        val duration = (endTime - startTime).nanos
        println(s"time for query n4topk ${config.nFactor} :${duration.toMillis} milliseconds")

        val startTime2 = System.nanoTime()

        val recallDF = reResult.select("indexIdTest", "indexIdTrainList").rdd.map(row => {
          val indexIdTest = row.getAs[Int]("indexIdTest")
          val indexIdTrainList = row.getAs[Seq[Int]]("indexIdTrainList").toArray
          val updatedList = indexIdTrainList.map(_ - 1)
          val count = float2DDataNeighbor(indexIdTest - 1).take(config.topk).count(updatedList.contains)
          val recall = count.toDouble / config.topk
          (recall, 1)
        }).reduce((acc1, acc2) => {
          (acc1._1 + acc2._1, acc1._2 + acc2._2)
        })

        val avgRecall = recallDF._1 / recallDF._2
        println(s"recall rate = $avgRecall")

        val endTime2 = System.nanoTime()
        val duration2 = (endTime2 - startTime2).nanos
        println(s"time for sort:${duration2.toMillis} milliseconds")

      case None => println("命令解析失败")
    }
  }
}
