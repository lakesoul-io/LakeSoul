/**
  * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
  * See LICENSE in the project root for license information.
  */
package org.apache.spark.ml.lakesoul.scanns

import org.apache.spark.ml.linalg.Vector

object Types extends Serializable {
  type ItemId = Long
  type Item = (ItemId, Vector)
  type ItemIdDistancePair = (ItemId, Double)
  type BandedHashes = Array[Array[Int]]
}
