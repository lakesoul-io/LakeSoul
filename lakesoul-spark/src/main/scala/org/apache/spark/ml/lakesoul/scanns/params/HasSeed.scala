/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.params

import org.apache.spark.ml.param.{LongParam, Params}

/**
  * Trait for shared param seed. Default is current time in milliseconds
  */
trait HasSeed extends Params {

  /**
    * Param for random seed.
    * @group param
    */
  final val seed: LongParam = new LongParam(this, "seed", "random seed")

  setDefault(seed, System.currentTimeMillis())

  /** @group getParam */
  final def getSeed: Long = $(seed)
}
