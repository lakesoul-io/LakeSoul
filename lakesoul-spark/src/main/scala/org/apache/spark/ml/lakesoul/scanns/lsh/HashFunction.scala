/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.lsh

import org.apache.spark.ml.linalg.Vector

private[scanns] abstract class HashFunction extends Serializable {

  /**
    * Compute the hash signature of the supplied vector
    */
  def compute(v: Vector): Array[Int]
}
