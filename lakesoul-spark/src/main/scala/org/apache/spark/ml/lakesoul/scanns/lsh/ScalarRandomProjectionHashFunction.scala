/**
  * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
  * See LICENSE in the project root for license information.
  */
package org.apache.spark.ml.lakesoul.scanns.lsh

import org.apache.spark.ml.lakesoul.scanns.linalg.RandomProjection
import org.apache.spark.ml.linalg.Vector

class ScalarRandomProjectionHashFunction(rp: RandomProjection, width: Double) extends HashFunction with Serializable {
  private var printCount: Int = 0
  private var maxPrints: Int = 1

  def setMaxPrints(n: Int): this.type = {
    maxPrints = n
    this
  }

  /**
    * Compute the hash signature of the supplied vector
    */
  override def compute(v: Vector): Array[Int] = {
    val project = rp.project(v)

    if (maxPrints > 0 && printCount < maxPrints) {
      //      println(s"========== ScalarRandomProjectionHashFunction compute printCount=$printCount ========== \n $project")
      printCount += 1
    }

    project.toArray.map(_ / width).map(math.floor).map(_.toInt)
  }

  def computeWithBias(v: Vector, bias: Int): Array[Int] = {
    compute(v).map(_ + bias)
  }
}

