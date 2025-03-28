/**
  * Copyright 2023 LakeSoul Contributors. Licensed under the Apache License, Version 2.0.
  */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import java.util.Random
import org.apache.spark.ml.lakesoul.scanns.linalg.RandomProjection
import org.apache.spark.ml.lakesoul.scanns.lsh.ScalarRandomProjectionHashFunction
import org.apache.spark.ml.lakesoul.scanns.model.{LSHNearestNeighborSearchModel, LakeSoulLSHNearestNeighborSearchModel, LakeSoulRandomProjectionModel}
import org.apache.spark.ml.lakesoul.scanns.params.ScalarRandomProjectionLSHNNSParams
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
  * This is the LakeSoul implementation for LSH-based nearest neighbor search.
  * It is based on the LinkedIn implementation of L2ScalarRandomProjectionNNS.
  *
  * It creates a [[LakeSoulRandomProjectionModel]] that can be used for
  * performing approximate nearest neighbor search in l2 distance space. The model parameters can be set using the
  * provided setters and then calling [[createModel()]] providing it the input dimension. This creates the hash
  * functions which in this case are scalar-[[RandomProjection]] where each element of the projection matrix is sampled
  * from a gaussian distribution.
  */
class LakeSoulRandomProjectionNNS(override val uid: String = Identifiable.randomUID("LakeSoulRandomProjectionLSH"))
  extends LakeSoulLSHNearestNeighborSearch[LakeSoulRandomProjectionModel] with ScalarRandomProjectionLSHNNSParams {
  override def setNumHashes(n: Int): this.type = super.setNumHashes(n)

  override def setJoinParallelism(parallelism: Int): this.type = super.setJoinParallelism(parallelism)

  override def setBucketLimit(limit: Int): this.type = super.setBucketLimit(limit)

  override def setSignatureLength(r: Int): this.type = super.setSignatureLength(r)

  def setBucketWidth(w: Double): this.type = set(bucketWidth, w)

  /**
    * Create a [[LakeSoulRandomProjectionModel]]
    *
    * @param dimension The dimension of vectors in the input dataset
    * @return A new [[LakeSoulRandomProjectionModel]]
    */
  override def createModel(dimension: Int): LakeSoulLSHNearestNeighborSearchModel[LakeSoulRandomProjectionModel] = {
    val rand = new Random($(seed))
    val randomProjections: Array[ScalarRandomProjectionHashFunction] = {
      Array.fill($(numHashes) / $(signatureLength)) {
        new ScalarRandomProjectionHashFunction(
          RandomProjection.generateGaussian(dimension, $(signatureLength), rand),
          $(bucketWidth)
        )
      }
    }
    val model = new LakeSoulRandomProjectionModel(uid, randomProjections)
    copyValues(model)
    model
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
} 