/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.distance

import breeze.linalg.norm
import org.apache.spark.ml.lakesoul.scanns.distance.DistanceMetric.DistanceMetric
import org.apache.spark.ml.lakesoul.scanns.utils.VectorUtils
import org.apache.spark.ml.linalg.Vector

object L2Distance extends Distance {
  override val metric: DistanceMetric = DistanceMetric.l2

  /**
    * Compute distance between x and y
    * @param x input vector
    * @param y input vector
    * @return l2 distance between x and y
    */
  override def compute(x: Vector, y: Vector): Double = {
    // TODO What are the performance implications here? Can we do better using direct computation rather than using breeze
    norm(VectorUtils.toBreeze(x) - VectorUtils.toBreeze(y), 2.0)
  }
}
