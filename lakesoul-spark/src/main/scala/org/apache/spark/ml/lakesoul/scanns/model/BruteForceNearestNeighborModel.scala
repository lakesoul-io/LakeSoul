/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.{Item, ItemId, ItemIdDistancePair}
import org.apache.spark.ml.lakesoul.scanns.distance.{Distance, DistanceMetric}
import org.apache.spark.ml.lakesoul.scanns.params.BruteForceNNSParams
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.rdd.MLPairRDDFunctions.fromPairRDD
import org.apache.spark.rdd.RDD

/**
  * Brute force all-pairs nearest neighbor search
  */
class BruteForceNearestNeighborModel(val uid: String = Identifiable.randomUID("BruteForceNNS"))
  extends NearestNeighborModel[BruteForceNearestNeighborModel] with BruteForceNNSParams {

  // we want this to be instantiated lazily because the distance metric will take the default value and not have been
  // set correctly until copyValues is called and the distance metric is set appropriately. See
  // [[org.apache.spark.ml.lakesoul.scanns.algorithm.BruteForceNNS#createModel]] for details
  lazy val distance: Distance = DistanceMetric.getDistance($(distanceMetric))

  override def getAllNearestNeighbors(srcItems: RDD[Item], candidatePool: RDD[Item], k: Int): RDD[(ItemId, ItemId, Double)] = {
    srcItems.cartesian(candidatePool).flatMap { case ((id1, vec1), (id2, vec2)) =>
      if (id1 == id2) {
        None
      } else {
        Some((id1, (id2, distance.compute(vec1, vec2))))
      }
    }
    .topByKey(k)(Ordering.by[ItemIdDistancePair, Double](-_._2)) // we want k pairs with the smallest distance
    .flatMap {
      case (id, topk) => topk.map { case (candidateId, dist) => (id, candidateId, dist) }
    }
  }

  override def getNearestNeighbors(key: Vector, candidatePool: RDD[Item], k: Int): Array[ItemIdDistancePair] = {
    val keyB = candidatePool.sparkContext.broadcast(key)
    candidatePool
      .mapValues(distance.compute(keyB.value, _))
      .takeOrdered(k)(Ordering.by[ItemIdDistancePair, Double](_._2))
  }

  override def getSelfAllNearestNeighbors(items: RDD[Item], k: Int): RDD[(ItemId, ItemId, Double)] = {
    getAllNearestNeighbors(items, items, k)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
