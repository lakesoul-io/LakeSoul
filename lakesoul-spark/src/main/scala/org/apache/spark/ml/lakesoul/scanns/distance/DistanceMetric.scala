/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.distance

/**
 * Enum of the currently supported distance measures with a method to retrieve the appropriate measure from a string
 */
object DistanceMetric extends Enumeration {
  type DistanceMetric = Value
  val jaccard, cosine, l2 = Value

  def getDistance(metric: String): Distance = {
    DistanceMetric.withName(metric.toLowerCase) match {
      case DistanceMetric.jaccard => JaccardDistance
      case DistanceMetric.cosine => CosineDistance
      case DistanceMetric.l2 => L2Distance
    }
  }
}
