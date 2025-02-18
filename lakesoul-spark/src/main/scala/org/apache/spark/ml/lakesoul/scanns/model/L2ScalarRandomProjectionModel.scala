/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.BandedHashes
import org.apache.spark.ml.lakesoul.scanns.distance.{Distance, L2Distance}
import org.apache.spark.ml.lakesoul.scanns.lsh.ScalarRandomProjectionHashFunction
import org.apache.spark.ml.lakesoul.scanns.params.ScalarRandomProjectionLSHNNSParams
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
 * Model to perform nearest neighbor search in the L2 distance space
 *
 * @param hashFunctions The random projections that will be used for LSH
 */
class L2ScalarRandomProjectionModel(val uid: String = Identifiable.randomUID("ScalarRandomProjectionLSH"),
                                    private val hashFunctions: Array[ScalarRandomProjectionHashFunction])
  extends LSHNearestNeighborSearchModel[L2ScalarRandomProjectionModel]
    with ScalarRandomProjectionLSHNNSParams {

  override val distance: Distance = L2Distance

  override private[scanns] def getHashFunctions: Array[ScalarRandomProjectionHashFunction] = hashFunctions

  /**
   * Given an input vector, get the banded hashes by hashing it using the hash functions
   *
   * @param x input vector
   * @return banded hashes
   */
  override def getBandedHashes(x: Vector): BandedHashes = {
    hashFunctions.map(_.compute(x))
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
