/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.BandedHashes
import org.apache.spark.ml.lakesoul.scanns.distance.{CosineDistance, Distance}
import org.apache.spark.ml.lakesoul.scanns.lsh.{HashFunction, SignRandomProjectionHashFunction}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
 * Model to perform nearest neighbor search in cosine distance space
 *
 * @param hashFunctions Array of random projections that will be used for performing LSH
 */
class CosineSignRandomProjectionModel(val uid: String = Identifiable.randomUID("SignRandomProjectionLSH"),
                                      private[scanns] val hashFunctions: Array[SignRandomProjectionHashFunction])
  extends LSHNearestNeighborSearchModel[CosineSignRandomProjectionModel] {

  override val distance: Distance = CosineDistance

  override private[scanns] def getHashFunctions: Array[SignRandomProjectionHashFunction] = hashFunctions

  /**
   * Given an input vector, get the banded hashes by hashing it using the hash functions
   *
   * @param x input vector
   * @return banded hashes
   */
  override def getBandedHashes(x: Vector): BandedHashes = {
    hashFunctions.map { h =>
      Array(
        h.compute(x)
          .reduceLeft((x, y) => (x << 1) + y) // Convert the binary signature to an int using Horner's evaluation
      )
    }
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
