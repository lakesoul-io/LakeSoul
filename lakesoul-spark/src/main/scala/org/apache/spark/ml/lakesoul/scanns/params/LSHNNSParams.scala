/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.params

import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamValidators, Params}

trait LSHNNSParams extends Params {
  /**
   * Param for the number of hashes used in LSH OR-amplification.
   *
   * LSH OR-amplification can be used to reduce the false negative rate. Higher values for this param lead to a
   * reduced false negative rate, at the expense of added computational complexity. Should ideally be set to be a
   * multiple of [[signatureLength]]
   */
  private[scanns] val numHashes:  IntParam =
    new IntParam(this, "numHashes", "Number of hashes to use for estimation", ParamValidators.gt(0))

  final def getNumHashes: Int = $(numHashes)

  setDefault(numHashes -> 256)

  /**
   * Param for number of hash functions used in LSH AND-amplification
   *
   * LSH AND-amplification can be used to reduce the false-positive rate. Upper bounded by value of [[numHashes]].
   * Should ideally be set to a divisor of [[numHashes]].
   */
  private[scanns] val signatureLength: IntParam =
    new IntParam(
      this,
      "signatureLength",
      "Length of signature, also referred to as band size in literature",
      ParamValidators.gt(0))

  /** @group getParam */
  final def getSignatureLength: Int = $(signatureLength)

  setDefault(signatureLength -> 16)

  /**
   * The parallelism of the custom join used to perform nearest neighbor computation.
   *
   * Higher values ensure that a single partition does not process too much data but at the same time, higher values
   * increase task management overhead for Spark. Must be set carefully depending on the volume of data being processed
   */
  private[scanns] val joinParallelism: IntParam = new IntParam(this, "joinParallelism",
    "Paralellism of the join for nearest neighbor search", ParamValidators.gt(0))

  /** @group getParam */
  final def getJoinParallelism: Int = $(joinParallelism)

  setDefault(joinParallelism -> 500)

  /**
   * Bucket limit is critical to address the bucket skew issue. When a bucket contains more items than the limit set
   * by this parameter, we reservoir sample bucketLimit number of items from the incoming stream. This means that a
   * certain number of items (particularly, bucketLimit number of items) will get randomly sampled to be in the bucket
   * and the rest of the items in that bucket will be discarded.
   *
   * This is crucial because within a bucket, we perform brute force search to calculate nearest neighbors and so we
   * cannot allow buckets to grow arbitrarily large without affecting performance drastically
   */
  private[scanns] val bucketLimit: IntParam =
    new IntParam(
      this,
      "bucketLimit",
      "To prevent a single bucket from getting too big, set bucketLimit to be the upper limit on the bucket size. " +
        "If the number of items mapped to this bucket are more than the set limit, we use reservoir sampling to select " +
        "the ones that get preserved while the rest get discarded",
      ParamValidators.gt(0))

  /** @group getParam */
  final def getBucketLimit: Int = $(bucketLimit)

  setDefault(bucketLimit -> 1000)

  /**
    * To address bucket skew, we set a limit on number of items that we accommodate in a bucket. This parameter controls
    * how we reach that limit. There are two ways to accomplish the limit:
    *  1. We can just take the first items that we encounter before we reach the limit and then discard the rest
    *  2. We can look at all the items that fall in a bucket and take a uniform sample of the required size
    * For 1, set the parameter to false
    * For 2, set the parameter to true
    */
  private[scanns] val shouldSampleBuckets: BooleanParam =
    new BooleanParam(
      this,
      "shouldSampleBuckets",
      "To prevent a single bucket from getting too big, we set a limit on how many elements we consider from a given " +
        "bucket. This parameter controls how we reach this limit. If set to false, we simply fill the bucket till it " +
        "reaches the limit and ignore all the remaining items that fall in the bucket. If set to true, we look at all " +
        "the elements that fall in the bucket and take a uniform sample from it to populate the bucket using reservoir " +
        "sampling. ")

  /** @group getParam */
  final def getShouldSampleBuckets: Boolean = $(shouldSampleBuckets)

  setDefault(shouldSampleBuckets -> false)

  /**
   * Since the parallelism of the join can be high, the output produced by the join operation will have a very high
   * number of partitions even though output of individual partitions isn't large. The parameter dictates into how many
   * partitions the output of the join be repartitioned. This is so that if the user tried to directly write the output
   * of nearest neighbors to HDFS, the number of files it will be split into will be a reasonable number.
   */
  private[scanns] val numOutputPartitions: IntParam =
    new IntParam(this, "numOutputPartitions", "Number of partitions in the output", ParamValidators.gt(0))

  /** @group getParam */
  final def getNumOutputPartitions: Int = $(numOutputPartitions)

  setDefault(numOutputPartitions -> 100)
}
