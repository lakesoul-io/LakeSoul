/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.lsh

import org.apache.spark.ml.linalg.Vector

class MinHashFunction(minHashes: Array[(Int, Int)]) extends HashFunction with Serializable {
  /**
    * Compute the hash signature of the supplied vector
    */
  override def compute(v: Vector): Array[Int] = {
    minHashes.map { case (a, b) =>
      v.toSparse.indices.map { elem: Int =>
        ((1 + elem) * a + b) % MinHashFunction.LARGE_PRIME
      }.min
    }
  }
}

object MinHashFunction {
  val LARGE_PRIME: Int = 2038074743
}
