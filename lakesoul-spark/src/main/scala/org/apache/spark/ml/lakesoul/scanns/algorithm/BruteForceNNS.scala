/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.algorithm

import org.apache.spark.ml.lakesoul.scanns.model.BruteForceNearestNeighborModel
import org.apache.spark.ml.lakesoul.scanns.params.BruteForceNNSParams
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.util.Identifiable

class BruteForceNNS(override val uid: String = Identifiable.randomUID("BruteForceNNS"))
  extends BruteForceNNSParams with Serializable {

  def setDistanceMetric(metric: String): this.type = {
    set(distanceMetric -> metric)
  }

  def createModel(): BruteForceNearestNeighborModel = {
    val model = new BruteForceNearestNeighborModel(uid)
    copyValues(model)
    model
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)
}
