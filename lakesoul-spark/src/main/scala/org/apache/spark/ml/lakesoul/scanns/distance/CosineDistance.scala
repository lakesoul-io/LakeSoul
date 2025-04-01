/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.distance

import org.apache.spark.ml.lakesoul.scanns.distance.DistanceMetric.DistanceMetric
import org.apache.spark.ml.lakesoul.scanns.utils.VectorUtils
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Compute cosine distance between two given vectors (1 - cosine similarity)
  *
  * Note that this is not really a distance measure as per definition of distance metrics but it is not really important
  * since it only gets invoked for a potential candidate pair. The actual LSH computation still happens over the
  * true cosine distance measure defined in terms of angle between the vectors
  */
object CosineDistance extends Distance {
  override val metric: DistanceMetric = DistanceMetric.cosine

  /**
    * Compute distance between x and y
    *
    * @param x input vector
    * @param y input vector
    * @return (1 - cosineSimilarity(x, y))
    */
  override def compute(x: Vector, y: Vector): Double = {
    require(x.numNonzeros > 0 || y.numNonzeros > 0, "Cosine distance of an empty vector with any vector is undefined")
    1.0 - (math.abs(VectorUtils.dot(x, y)) / (Vectors.norm(x, 2) * Vectors.norm(y, 2)))
  }
}
