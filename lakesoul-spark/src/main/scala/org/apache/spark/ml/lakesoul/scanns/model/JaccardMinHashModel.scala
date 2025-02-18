/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.BandedHashes
import org.apache.spark.ml.lakesoul.scanns.distance.{Distance, JaccardDistance}
import org.apache.spark.ml.lakesoul.scanns.lsh.{HashFunction, MinHashFunction}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
 * Model to perform nearest neighbor search in the jaccard space
 *
 * @param hashFunctions Array of pairs of integers that will be used for implementing minwise independent permutations
 */
class JaccardMinHashModel(val uid: String = Identifiable.randomUID("MinhashLSH"),
                          private[scanns] val hashFunctions: Array[MinHashFunction])
  extends LSHNearestNeighborSearchModel[JaccardMinHashModel] {

  override val distance: Distance = JaccardDistance

  override private[scanns] def getHashFunctions: Array[MinHashFunction] = hashFunctions

  /**
   * Given an input vector, get the banded hashes by hashing it using the hash functions
   *
   * @param x input vector
   * @return banded hashes
   */
  override def getBandedHashes(x: Vector): BandedHashes = {
    require(x.numNonzeros > 0, "Must have at least 1 non zero entry.")
    hashFunctions.map(_.compute(x))
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
