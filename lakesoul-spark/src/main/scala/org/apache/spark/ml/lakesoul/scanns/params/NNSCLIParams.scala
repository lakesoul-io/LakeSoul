///**
// * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
// * See LICENSE in the project root for license information.
// */
//package org.apache.spark.ml.lakesoul.scanns.params
//
//import scopt.OptionParser
//
//class NNSCLIParams {
//  /**
//   * Path to dataset oontaining items for which nearest neighbors are to be found
//   */
//  var srcItemsPath = ""
//
//  /**
//   * Path to the dataset containing items among which nearest neighbors are to be searched. This is optional. If
//   * unspecified, the search will be performed within the items in [[srcItemsPath]] dataset
//   */
//  var candidatePoolPath = ""
//
//  /**
//   * Name of the column that contains the item id
//   */
//  var idColumnName = "memberId"
//
//  /**
//   * Name of the feature bag column i.e the column containing a bag of (name, term, value) records
//   */
//  var attributesColumnName = "attributes"
//
//  /**
//   * Path containing a list of features, one per line with [[org.apache.spark.ml.lakesoul.scanns.utils.CommonConstantsAndUtils.separator]]
//   * separated name and term. This can be useful if you do not wish to use all the features within the feature bag to
//   * search for nearest neighbors
//   */
//  var attributesNameTermPath: String = ""
//
//  /**
//   * The algorithm that should be used for nearest neighbor search. The options are `brute`, `signRP`, `scalarRP` and
//   * `minhash`
//   */
//  var algorithm: String = "signRP"
//
//  /**
//   * This is relevant only if the algorithm is `brute`. For other choices, the distance is automatically decided by the
//   * chosen algorithm. Therefore, in those cases, this parameter will be ignored
//   */
//  var distanceMetric: String = "cosine"
//
//  /**
//   * The path where the output should be stored
//   */
//  var outputPath = ""
//
//  /**
//   * The number of output files we want in the output
//   */
//  var numOutputFiles = 50
//
//  /**
//   * Number of hashes used in LSH OR-amplification.
//   *
//   * LSH OR-amplification can be used to reduce the false negative rate. Higher values for this param lead to a
//   * reduced false negative rate, at the expense of added computational complexity. Should ideally be set to be a
//   * multiple of [[signatureLength]]
//   */
//  var numHashTables = 100
//
//  /**
//   * Number of hash functions used in LSH AND-amplification
//   *
//   * LSH AND-amplification can be used to reduce the false-positive rate. Upper bounded by value of [[numHashTables]].
//   * Should ideally be set to a divisor of [[numHashTables]].
//   */
//  var signatureLength = 1
//
//  /**
//   * The length of each hash bucket, a larger bucket lowers the false negative rate. This is only used in case the
//   * algorithm is scalarRP and ignored otherwise. The number of buckets will be
//   * `(max L2 norm of input vectors) / bucketLength`.
//   *
//   * If input vectors are normalized, 1-10 times of pow(numRecords, -1/inputDim) would be a reasonable value
//   */
//  var bucketWidth = 0.5
//
//  /**
//   * The parallelism of the custom join used to perform nearest neighbor computation.
//   *
//   * Higher values ensure that a single partition does not process too much data but at the same time, higher values
//   * increase task management overhead for Spark. Must be set carefully depending on the volume of data being processed
//   */
//  var joinParallelism = 50000
//
//  /**
//   * Bucket limit is critical to address the bucket skew issue. When a bucket contains more items than the limit set
//   * by this parameter, we reservoir sample bucketLimit number of items from the incoming stream. This means that a
//   * certain number of items (particularly, bucketLimit number of items) will get randomly sampled to be in the bucket
//   * and the rest of the items in that bucket will be discarded.
//   *
//   * This is crucial because within a bucket, we perform brute force search to calculate nearest neighbors and so we
//   * cannot allow buckets to grow arbitrarily large without affecting performance drastically
//   */
//  var bucketLimit = 10000
//
//  /**
//   * For enforcing the bucket limit, we have two strategies.
//   *   1. We can just take the first items that we encounter before we reach the limit and then discard the rest
//   *   2. We can look at all the items that fall in a bucket and take a uniform sample of the required size
//   * For 1, set the parameter to false. For 2, set the parameter to true.
//   */
//  var shouldSampleBuckets = false
//
//  /**
//   * Number of candidates required for each query item
//   */
//  var numCandidates = 100
//
//  override def toString: String = {
//    s"srcItemsPath: $srcItemsPath\n" +
//      s"candidatePoolPath: $candidatePoolPath\n" +
//      s"idColumnName: $idColumnName\n" +
//      s"attributeColumnName: $attributesColumnName\n" +
//      s"attributesNameTermPath: $attributesNameTermPath\n" +
//      s"algorithm: $algorithm\n" +
//      s"distanceMetric: $distanceMetric\n" +
//      s"outputPath: $outputPath\n" +
//      s"numOutputFiles: $numOutputFiles\n" +
//      s"numHashTables: $numHashTables\n" +
//      s"signatureLength: $signatureLength\n" +
//      s"bucketWidth: $bucketWidth\n" +
//      s"joinParallelism: $joinParallelism\n" +
//      s"bucketLimit: $bucketLimit\n" +
//      s"shouldSampleBuckets: $shouldSampleBuckets\n" +
//      s"numCandidates: $numCandidates\n"
//  }
//}
//
//object NNSCLIParams {
//  def parseFromCommandLine(args: Array[String]): NNSCLIParams = {
//    val params = new NNSCLIParams()
//
//    val parser = new OptionParser[Unit]("Candidate selection params") {
//      opt[String]("src-items-path")
//        .required()
//        .text("The path to the set of items for which candidates are sought")
//        .foreach(x => params.srcItemsPath = x)
//      opt[String]("candidate-pool-path")
//        .text("The path to the set of items from which the candidates will be chosen. If unspecified, candidates will " +
//          "be searched within the src items themselves")
//        .foreach(x => params.candidatePoolPath = x)
//      opt[String]("id-column-name")
//        .optional()
//        .text(s"The name of the id column in (both) the datasets. Default: ${params.idColumnName}")
//        .foreach(x => params.idColumnName = x)
//      opt[String]("attributes-column-name")
//        .optional()
//        .text(s"The name of the attributes column in (both) the datasets. Default: ${params.attributesColumnName}")
//        .foreach(x => params.attributesColumnName = x)
//      opt[String]("attributes-name-term-path")
//        .text("The path to the selected attributes. While this argument is optional, if it is absent, it is assumed " +
//          "inputs are hash exploded")
//        .foreach(x => params.attributesNameTermPath = x)
//      opt[String]("algorithm")
//        .text("Algorithm to use for nearest neighbor search. Options are brute, minhash, signRP or scalarRP")
//        .foreach(x => params.algorithm = x)
//      opt[String]("distance-metric")
//        .text("Distance metric to use in case brute force search is being performed. Will be ignored otherwise." +
//          s"Options are cosine, l2 and jaccard. Default: ${params.distanceMetric}")
//        .foreach(x => params.distanceMetric = x)
//      opt[String]("output-path")
//        .required()
//        .text(s"Path to directory where the output will be stored")
//        .foreach(x => params.outputPath = x)
//      opt[Int]("num-output-files")
//        .optional()
//        .text(s"Number of files for the candidates that will be output. Default: ${params.numOutputFiles}")
//        .foreach(x => params.numOutputFiles = x)
//      opt[Int]("num-hash-tables")
//        .optional()
//        .text(s"Number of hash tables to be used for computing signatures. More the number of hash tables, better " +
//          s"the estimation of the distance metric but more expensive the computation. Default: ${params.numHashTables}")
//        .foreach(x => params.numHashTables = x)
//      opt[Int]("signature-length")
//        .optional()
//        .text(s"The number of hashes in our definition of a 'hash bucket'. Default: ${params.signatureLength}")
//        .foreach(x => params.signatureLength = x)
//      opt[Double]("bucket-width")
//        .optional()
//        .text(s"Bucket width in case L2 distance LSH is being performed. Is ignored otherwise")
//        .foreach(x => params.bucketWidth = x)
//      opt[Int]("join-parallelism")
//        .optional()
//        .text("The parallelism at which the join will be performed. This is a critical parameter in terms of making " +
//          "the computation faster. It should be set such that each task gets a fairly small, manageable amount of " +
//          "data. At the same time, if we indiscrimanately increase it for small datasets, there is a lot of overhead " +
//          s"in creating and closing tasks. Default: ${params.joinParallelism}")
//        .foreach(x => params.joinParallelism = x)
//      opt[Int]("bucket-limit")
//        .optional()
//        .text("For certain hash buckets, it is possible that there are hundreds of thousands of items that fall in " +
//          "it. This creates a high memory overhead for the data structures that we maintain and potentially a lot " +
//          "of GC. This sets a limit on how many candidates we allow to lie in a single hash bucket. Whatever limit " +
//          "is imposed, after we observe that many number of items, we ignore all the rest. " +
//          s"Default: ${params.bucketLimit}")
//        .foreach(x => params.bucketLimit = x)
//      opt[Boolean]("should-sample-buckets")
//        .optional()
//        .text("For enforcing the bucket limit, we have two strategies. Either we can just take the first items that we " +
//          "encounter before we reach the limit and then discard the rest or we can look at all the items that fall in " +
//          "a bucket and take a uniform sample of the required size. For the former strategy, this parameter should be " +
//          s"set to false. For the latter, it should be set to true. Default: ${params.shouldSampleBuckets}")
//        .foreach(x => params.shouldSampleBuckets = x)
//      opt[Int]("num-candidates")
//        .optional()
//        .text("Number of candidates being sought per source item. This is an upper bound and it is possible that " +
//          "we do not see as many candidates falling in the same bucket as a particular source item. " +
//          s"Default: ${params.numCandidates}")
//        .foreach(x => params.numCandidates = x)
//
//      help("help").text("Prints usage text")
//    }
//
//    if (!parser.parse(args)) {
//      throw new IllegalArgumentException(s"Parsing the command line arguments failed.\n" +
//        s"(${args.mkString(", ")}),\n ${parser.usage}")
//    }
//
//    params
//  }
//}
