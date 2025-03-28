/**
  * Copyright 2023 LakeSoul Contributors. Licensed under the Apache License, Version 2.0.
  */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.BandedHashes
import org.apache.spark.ml.lakesoul.scanns.distance.{CosineDistance, Distance, DistanceMetric, L2Distance, NormalizedL2Distance}
import org.apache.spark.ml.lakesoul.scanns.lsh.ScalarRandomProjectionHashFunction
import org.apache.spark.ml.lakesoul.scanns.params.ScalarRandomProjectionLSHNNSParams
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

/**
  * LakeSoul model to perform nearest neighbor search in the L2 distance space.
  * This is a fork of L2ScalarRandomProjectionModel customized for LakeSoul's requirements.
  *
  * @param hashFunctions The random projections that will be used for LSH
  */
class LakeSoulRandomProjectionModel(val uid: String = Identifiable.randomUID("LakeSoulRandomProjectionLSH"),
                                    private val hashFunctions: Array[ScalarRandomProjectionHashFunction])
  extends LakeSoulLSHNearestNeighborSearchModel[LakeSoulRandomProjectionModel]
    with ScalarRandomProjectionLSHNNSParams {

  //  override val distance: Distance = NormalizedL2Distance
  lazy val distance: Distance = DistanceMetric.getDistance($(distanceMetric))

  val r = new scala.util.Random()

  override private[ml] def getHashFunctions: Array[ScalarRandomProjectionHashFunction] = hashFunctions

  /**
    * Given an input vector, get the banded hashes by hashing it using the hash functions
    *
    * @param x input vector
    * @return banded hashes
    */
  override def getBandedHashes(x: Vector): BandedHashes = {
    hashFunctions.map(_.compute(x))
  }

  override def getBandedHashesWithBias(x: Vector, bias: Int): BandedHashes = {
    hashFunctions.flatMap { h =>
      val original = h.compute(x)
      val results = new Array[Array[Int]](bias + 1)
      results(0) = original
      
      for (i <- 1 to bias) {
        val modified = results(i-1).clone()
        val pos = r.nextInt(modified.length) // Random position
        modified(pos) += (if (r.nextBoolean()) 1 else -1) // Randomly add or subtract 1
        results(i) = modified
      }
      
      results
    }
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
} 