/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.utils

import breeze.linalg.{Vector => BV}
import org.apache.spark.ml.linalg.Vector


object VectorUtils {
  /**
    * Compute the dot product between two vectors
    */
  def dot(x: Vector, y: Vector): Double = {
    toBreeze(x) dot toBreeze(y)
  }

  /**
    * Convert a Spark vector to a Breeze vector to access
    * vector operations that Spark doesn't provide.
    */
  def toBreeze(x: Vector): BV[Double] = {
    // TODO What are the performance implications here? Can we do better using conversion to breeze sparse vectors?
    BV[Double](x.toArray)
  }
}
