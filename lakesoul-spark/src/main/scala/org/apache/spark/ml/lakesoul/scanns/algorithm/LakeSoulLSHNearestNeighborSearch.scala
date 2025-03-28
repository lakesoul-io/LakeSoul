/**
  * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
  * See LICENSE in the project root for license information.
  */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import org.apache.spark.ml.lakesoul.scanns.model.{LSHNearestNeighborSearchModel, LakeSoulLSHNearestNeighborSearchModel}
import org.apache.spark.ml.lakesoul.scanns.params.{BruteForceNNSParams, DistanceParams, HasSeed, LSHNNSParams}


/**
  * Interface for creating a Nearest neighbor search model. It is expected that the user sets the parameters first
  * using the provided setters and then invokes [[createModel()]]. If not, the default values for the parameters will
  * be used
  *
  * @tparam T
  */
abstract class LakeSoulLSHNearestNeighborSearch[T <: LakeSoulLSHNearestNeighborSearchModel[T]]
  extends LSHNNSParams with Serializable with HasSeed with DistanceParams {
  /**
    * Create a new instance of concrete [[LakeSoulLSHNearestNeighborSearchModel]]
    *
    * @param dimension The dimension of vectors in the input dataset
    * @return A new [[LakeSoulLSHNearestNeighborSearchModel]]
    */
  def createModel(dimension: Int): LakeSoulLSHNearestNeighborSearchModel[T]

  def setNumHashes(n: Int): this.type = set(numHashes, n)

  def setJoinParallelism(parallelism: Int): this.type = set(joinParallelism, parallelism)

  def setBucketLimit(limit: Int): this.type = set(bucketLimit, limit)

  def setShouldSampleBuckets(sample: Boolean): this.type = set(shouldSampleBuckets, sample)

  def setSignatureLength(r: Int): this.type = set(signatureLength, r)

  def setNumOutputPartitions(n: Int): this.type = set(numOutputPartitions, n)

  def setSeed(value: Long): this.type = set(seed, value)

  def setDistanceMetric(value: String): this.type = set(distanceMetric, value)
}
