/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import org.apache.spark.ml.lakesoul.scanns.lsh.MinHashFunction
import org.apache.spark.ml.lakesoul.scanns.model.{JaccardMinHashModel, LSHNearestNeighborSearchModel}
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

import scala.util.Random

/**
  * This is the gateway that should be used for creating a [[JaccardMinHashModel]] that can be used for performing
  * approximate nearest neighbor search in jaccard distance space. The model parameters can be set using the
  * provided setters and then calling [[createModel()]] providing it the input dimension. This creates the hash
  * functions which in this case are simply min-wise independent permutations.
  *
  * Each hash function is composed of two numbers (a, b) and the mapping of each index is defined as (a * index + b)
  * modulo a large prime. This can be thought of roughly as the universal set containing a large number of elements
  * (the prime number used by the hash functions) and defining a permutation over this set.
  */
class JaccardMinHashNNS(override val uid: String = Identifiable.randomUID("MinhashLSH"))
  extends LSHNearestNeighborSearch[JaccardMinHashModel] {
  override def setNumHashes(n: Int): this.type = super.setNumHashes(n)

  override def setSignatureLength(r: Int): this.type = super.setSignatureLength(r)

  override def setJoinParallelism(parallelism: Int): this.type = super.setJoinParallelism(parallelism)

  override def setBucketLimit(limit: Int): this.type = super.setBucketLimit(limit)

  /**
    * Create a [[JaccardMinHashModel]]
    * @param dimension The dimension of vectors in the input dataset
    * @return A new [[JaccardMinHashModel]]
    */
  override def createModel(dimension: Int): LSHNearestNeighborSearchModel[JaccardMinHashModel] = {
    require(dimension <= MinHashFunction.LARGE_PRIME,
      s"Vector dimension $dimension exceeds the maximum supported ${MinHashFunction.LARGE_PRIME}.")
    val rand = new Random($(seed))
    val hashes: Array[MinHashFunction] = Array.fill($(numHashes) / $(signatureLength)) {
      new MinHashFunction(
        Array.fill($(signatureLength)) {
          (1 + rand.nextInt(MinHashFunction.LARGE_PRIME - 1), rand.nextInt(MinHashFunction.LARGE_PRIME - 1))
        }
      )
    }
    val model = new JaccardMinHashModel(uid, hashes)
    copyValues(model)
    model
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}