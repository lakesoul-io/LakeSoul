/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import org.apache.spark.ml.lakesoul.scanns.model.LSHNearestNeighborSearchModel
import org.apache.spark.ml.lakesoul.scanns.params.{HasSeed, LSHNNSParams}


/**
  * Interface for creating a Nearest neighbor search model. It is expected that the user sets the parameters first
  * using the provided setters and then invokes [[createModel()]]. If not, the default values for the parameters will
  * be used
  *
  * @tparam T
  */
abstract class LSHNearestNeighborSearch[T <: LSHNearestNeighborSearchModel[T]]
  extends LSHNNSParams with Serializable with HasSeed {
  /**
    * Create a new instance of concrete [[LSHNearestNeighborSearchModel]]
    *
    * @param dimension The dimension of vectors in the input dataset
    * @return A new [[LSHNearestNeighborSearchModel]]
    */
  def createModel(dimension: Int): LSHNearestNeighborSearchModel[T]

  def setNumHashes(n: Int): this.type = set(numHashes, n)

  def setJoinParallelism(parallelism: Int): this.type = set(joinParallelism, parallelism)

  def setBucketLimit(limit: Int): this.type = set(bucketLimit, limit)

  def setShouldSampleBuckets(sample: Boolean): this.type = set(shouldSampleBuckets, sample)

  def setSignatureLength(r: Int): this.type = set(signatureLength, r)

  def setNumOutputPartitions(n: Int): this.type = set(numOutputPartitions, n)

  def setSeed(value: Long): this.type = set(seed, value)
}
