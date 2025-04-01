/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.params

import org.apache.spark.ml.param.{Param, ParamValidators, Params}

trait DistanceParams extends Params {
  private[ml] val distanceMetric:  Param[String] = new Param(
    this,
    "distanceMetric",
    "Distance metric for brute force search",
    ParamValidators.inArray(Array("cosine", "jaccard", "l2")))

  final def getDistanceMetric: String = $(distanceMetric)

  setDefault(distanceMetric -> "cosine")
}
