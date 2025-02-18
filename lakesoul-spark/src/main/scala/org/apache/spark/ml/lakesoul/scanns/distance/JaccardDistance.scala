/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.distance

import org.apache.spark.ml.lakesoul.scanns.distance.DistanceMetric.DistanceMetric
import org.apache.spark.ml.linalg.Vector

object JaccardDistance extends Distance {
  override val metric: DistanceMetric = DistanceMetric.jaccard

  /**
    * Compute distance between x and y
    * @param x input set represented as vector (only the indices are relevant)
    * @param y input set represented as vector (only the indices are relevant)
    * @return jaccard distance between x and y
    */
  override def compute(x: Vector, y: Vector): Double = {
    require(x.numNonzeros > 0 && y.numNonzeros > 0, "Jaccard distance between two empty vectors is undefined")
    val xSet = x.toSparse.indices.toSet
    val ySet = y.toSparse.indices.toSet
    val intersection = xSet.intersect(ySet)
    val union = xSet.union(ySet)
    1.0 - (intersection.size / union.size.toDouble)
  }
}
