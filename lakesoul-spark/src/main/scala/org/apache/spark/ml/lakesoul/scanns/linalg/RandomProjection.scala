/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved. Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package org.apache.spark.ml.lakesoul.scanns.linalg

import java.util.Random

import org.apache.spark.ml.linalg.{DenseMatrix, Vector}

/**
  * Implementation of random projections
  *
  * @param matrix Matrix of random vectors with each vector being a row
  */
class RandomProjection(private[linalg] val matrix: DenseMatrix) extends Serializable {
  /**
    * Multiply query vector with the random matrix
    *
    * @param query vector to be projected
    * @return projection
    */
  def project(query: Vector): Vector = {
    matrix.multiply(query)
  }

  /**
    * Multiply query vector with the random matrix and then sign-project
    *
    * @param query vector to be projected
    * @return sign projection
    */
  def signProject(query: Vector): Array[Int] = {
    project(query).toArray.map(x => if (x > 0.0) 1 else 0)
  }
}

/**
  * Code for generation of random projection matrices
  */
object RandomProjection {
  /**
    * Generate a [[DenseMatrix]] consisting of `i.i.d.` gaussian random numbers
    *
    * @param inputVectorDim number of columns of the matrix
    * @param projectionDim number of rows of the matrix
    * @param rng a random number generator
    * @return [[RandomProjection]]
    */
  def generateGaussian(inputVectorDim: Int,
                       projectionDim: Int,
                       rng: Random): RandomProjection = {
    new RandomProjection(DenseMatrix.randn(projectionDim, inputVectorDim, rng))
  }
}
