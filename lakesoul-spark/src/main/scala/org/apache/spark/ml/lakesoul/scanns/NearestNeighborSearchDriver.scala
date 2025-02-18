///**
// * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
// * See LICENSE in the project root for license information.
// */
//package org.apache.spark.ml.lakesoul.scanns
//
//import com.databricks.spark.avro._
//import org.apache.spark.ml.lakesoul.scanns.Types.ItemId
//import org.apache.spark.ml.lakesoul.scanns.algorithm.{BruteForceNNS, CosineSignRandomProjectionNNS, JaccardMinHashNNS, L2ScalarRandomProjectionNNS}
//import org.apache.spark.ml.lakesoul.scanns.params.NNSCLIParams
//import org.apache.spark.ml.lakesoul.scanns.utils.CommonConstantsAndUtils
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.ml.linalg.{Vector, Vectors}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
//import org.apache.spark.sql.{Row, SaveMode, SparkSession}
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.collection.Map
//
///**
//  * This is a driver utility to run the nearest neighbor search algorithms
//  */
//object NearestNeighborSearchDriver {
//  /**
//    * Get feature index map from a pre-existing stored list of features. The stored list is expected to contain one
//    * feature per line with the name and term separated by [[CommonConstantsAndUtils.separator]]
//    *
//    * Note: The list of features is expected to fit in the spark driver memory
//    *
//    * @param nameTermPath Path containing stored list of features
//    * @param sc [[SparkContext]]
//    * @return Feature index map as a broadcast variable
//    */
//  def getFeatureIndexMap(nameTermPath: String, sc: SparkContext): Broadcast[Map[String, Int]] = {
//    sc.broadcast(
//      sc.textFile(nameTermPath)
//        .zipWithIndex()
//        .map(x => (x._1, x._2.toInt))
//        .collectAsMap())
//  }
//
//  /**
//    * Read an avro dataset containing the item id and a bag of (name, term, value) features into an RDD of item id
//    * and vector.
//    *
//    * The dataset may contain other fields
//    *
//    * @param path Path to the avro dataset
//    * @param idColumnName Name of the column containing item id
//    * @param attributesColumnName Name of feature bag column
//    * @param featureIndexMapB Broadcasted feature index map
//    * @param sparkSession [[SparkSession]]
//    * @return Key-value RDD of item id to the vector representation
//    */
//  def readDataset(path: String,
//                  idColumnName: String,
//                  attributesColumnName: String,
//                  featureIndexMapB: Broadcast[Map[String, Int]],
//                  sparkSession: SparkSession): RDD[(ItemId, Vector)] = {
//
//    val numFeatures = featureIndexMapB.value.size
//    sparkSession.read.avro(path)
//      .select(idColumnName, attributesColumnName)
//      .rdd
//      .map(r =>
//        (
//          r.get(0).toString.toLong,
//
//          Vectors.sparse(
//            numFeatures,
//            r.getSeq[Row](1).flatMap(f =>
//              featureIndexMapB
//                .value
//                .get(CommonConstantsAndUtils.getFeatureKey(f.getString(0), f.getString(1)))
//                .map((_, f.getDouble(2)))
//            )
//          )
//        )
//      )
//  }
//
//  /**
//    * Write the neighbors RDD back to HDFS as an avro dataset
//    * @param neighbors Neighbors RDD
//    * @param outputPath Path where the output should be stored. The content at the path will be overwritten
//    * @param sparkSession [[SparkSession]]
//    */
//  def writeOutput(neighbors: RDD[(Long, Long, Double)], outputPath: String, sparkSession: SparkSession): Unit = {
//    val output = neighbors.map{ case (x, y, z) => Row(x, y, z) }
//    val schema = StructType(Array(
//      StructField("itemId", LongType, nullable = false),
//      StructField("candidateId", LongType, nullable = false),
//      StructField("distance", DoubleType, nullable = false)))
//    sparkSession.createDataFrame(output, schema).write.mode(SaveMode.Overwrite).avro(outputPath)
//  }
//
//  /**
//   * Please refer [[NNSCLIParams]] for a detailed description of the expected arguments
//   *
//   * @param args args that will be parsed using scopt
//   */
//  def main(args: Array[String]): Unit = {
//    val params = NNSCLIParams.parseFromCommandLine(args)
//    val sparkConf = new SparkConf()
//    val sc = new SparkContext(sparkConf)
//    val spark = SparkSession
//      .builder()
//      .appName("NearestNeighborSearch")
//      .config(sparkConf)
//      .getOrCreate()
//    val indexMapB = getFeatureIndexMap(params.attributesNameTermPath, sc)
//    val numFeatures = indexMapB.value.size
//
//    val model = params.algorithm match {
//      case "brute" =>
//        new BruteForceNNS()
//          .setDistanceMetric(params.distanceMetric)
//          .createModel()
//
//      case "minhash" =>
//        new JaccardMinHashNNS()
//          .setNumHashes(params.numHashTables)
//          .setSignatureLength(params.signatureLength)
//          .setBucketLimit(params.bucketLimit)
//          .setShouldSampleBuckets(params.shouldSampleBuckets)
//          .setJoinParallelism(params.joinParallelism)
//          .setNumOutputPartitions(params.numOutputFiles)
//          .createModel(numFeatures)
//
//      case "signRP" =>
//        new CosineSignRandomProjectionNNS()
//          .setNumHashes(params.numHashTables)
//          .setSignatureLength(params.signatureLength)
//          .setBucketLimit(params.bucketLimit)
//          .setShouldSampleBuckets(params.shouldSampleBuckets)
//          .setJoinParallelism(params.joinParallelism)
//          .setNumOutputPartitions(params.numOutputFiles)
//          .createModel(numFeatures)
//
//      case "scalarRP" =>
//        new L2ScalarRandomProjectionNNS()
//          .setNumHashes(params.numHashTables)
//          .setSignatureLength(params.signatureLength)
//          .setBucketLimit(params.bucketLimit)
//          .setShouldSampleBuckets(params.shouldSampleBuckets)
//          .setJoinParallelism(params.joinParallelism)
//          .setBucketWidth(params.bucketWidth)
//          .setNumOutputPartitions(params.numOutputFiles)
//          .createModel(numFeatures)
//    }
//
//    val srcItems = readDataset(params.srcItemsPath, params.idColumnName, params.attributesColumnName, indexMapB, spark)
//    val neighbors = if (params.candidatePoolPath.nonEmpty && (! params.candidatePoolPath.equals(params.srcItemsPath))) {
//      val candidatePool =
//        readDataset(params.candidatePoolPath, params.idColumnName, params.attributesColumnName, indexMapB, spark)
//      model.getAllNearestNeighbors(srcItems, candidatePool, params.numCandidates)
//    } else {
//      model.getSelfAllNearestNeighbors(srcItems, params.numCandidates)
//    }
//    writeOutput(neighbors, params.outputPath, spark)
//  }
//}
