/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.distance

import org.apache.spark.ml.lakesoul.scanns.distance.DistanceMetric.DistanceMetric
import org.apache.spark.ml.linalg.Vector

/**
  * Trait that should be implemented by all distance measure used for computing distance between a candidate pair
  *
  * It is framed as distance (rather than similarity) to accommodate the euclidean distance measures (Lp norms).
  */
trait Distance extends Serializable {
  val metric: DistanceMetric
  /**
    * Compute distance between x and y
    * @param x input vector
    * @param y input vector
    * @return distance between x and y
    */
  def compute(x: Vector, y: Vector): Double
}
