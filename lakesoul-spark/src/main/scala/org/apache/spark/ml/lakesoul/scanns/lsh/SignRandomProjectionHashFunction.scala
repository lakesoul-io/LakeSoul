/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.lsh

import org.apache.spark.ml.lakesoul.scanns.linalg.RandomProjection
import org.apache.spark.ml.linalg.Vector

class SignRandomProjectionHashFunction(rp: RandomProjection) extends HashFunction with Serializable {
  /**
    * Compute the hash signature of the supplied vector
    */
  override def compute(v: Vector): Array[Int] = {
    rp.signProject(v) // output is an array of 0s and 1s
  }
}
