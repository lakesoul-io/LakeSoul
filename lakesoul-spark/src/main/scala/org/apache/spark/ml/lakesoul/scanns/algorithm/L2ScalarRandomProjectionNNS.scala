/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import java.util.Random

import org.apache.spark.ml.lakesoul.scanns.linalg.RandomProjection
import org.apache.spark.ml.lakesoul.scanns.lsh.ScalarRandomProjectionHashFunction
import org.apache.spark.ml.lakesoul.scanns.model.{L2ScalarRandomProjectionModel, LSHNearestNeighborSearchModel}
import org.apache.spark.ml.lakesoul.scanns.params.ScalarRandomProjectionLSHNNSParams
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
  * This is the gateway that should be used for creating a [[L2ScalarRandomProjectionModel]] that can be used for
  * performing approximate nearest neighbor search in l2 distance space. The model parameters can be set using the
  * provided setters and then calling [[createModel()]] providing it the input dimension. This creates the hash
  * functions which in this case are scalar-[[RandomProjection]] where each element of the projection matrix is sampled
  * from a gaussian distribution.
  */
class L2ScalarRandomProjectionNNS(override val uid: String = Identifiable.randomUID("ScalarRandomProjectionLSH"))
  extends LSHNearestNeighborSearch[L2ScalarRandomProjectionModel] with ScalarRandomProjectionLSHNNSParams {
  override def setNumHashes(n: Int): this.type = super.setNumHashes(n)

  override def setJoinParallelism(parallelism: Int): this.type = super.setJoinParallelism(parallelism)

  override def setBucketLimit(limit: Int): this.type = super.setBucketLimit(limit)

  override def setSignatureLength(r: Int): this.type = super.setSignatureLength(r)

  def setBucketWidth(w: Double): this.type = set(bucketWidth, w)

  /**
    * Create a [[L2ScalarRandomProjectionModel]]
    * @param dimension The dimension of vectors in the input dataset
    * @return A new [[L2ScalarRandomProjectionModel]]
    */
  override def createModel(dimension: Int): LSHNearestNeighborSearchModel[L2ScalarRandomProjectionModel] = {
    val rand = new Random($(seed))
    val randomProjections: Array[ScalarRandomProjectionHashFunction] = {
      Array.fill($(numHashes) / $(signatureLength)) {
        new ScalarRandomProjectionHashFunction(
          RandomProjection.generateGaussian(dimension, $(signatureLength), rand),
          $(bucketWidth)
        )
      }
    }
    val model = new L2ScalarRandomProjectionModel(uid, randomProjections)
    copyValues(model)
    model
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
