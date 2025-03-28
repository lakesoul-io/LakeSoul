/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.model

import org.apache.spark.ml.lakesoul.scanns.Types.{Item, ItemId, ItemIdDistancePair}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

trait NearestNeighborModel[T <: NearestNeighborModel[T]] extends Serializable {
  /**
    * Get k nearest neighbors to all items in srcItems dataset from the candidatePool dataset
    *
    * @param srcItems Items for which neighbors are to be found
    * @param candidatePool Items which are potential candidates
    * @param k number of nearest neighbors needed
    * @return nearest neighbors in the form (srcItemId, candidateItemId, distance)
    */
  def getAllNearestNeighbors(srcItems: RDD[Item], candidatePool: RDD[Item], k: Int): RDD[(ItemId, ItemId, Double)]

  /**
    * Get k nearest neighbors for all input items from within itself
    *
    * @param items Set of items
    * @param k number of nearest neighbors needed
    * @return nearest neighbors in the form (srcItemId, candidateItemId, distance)
    */
  def getSelfAllNearestNeighbors(items: RDD[Item], k: Int): RDD[(ItemId, ItemId, Double)]

  /**
    * Get k nearest neighbors to the query vector from the given items
    *
    * @param key query vector
    * @param items items to be searched for nearest neighbors
    * @param k number of nearest neighbors needed
    * @return array of (itemId, distance) tuples
    */
  def getNearestNeighbors(key: Vector, items: RDD[Item], k: Int): Array[ItemIdDistancePair]
}
