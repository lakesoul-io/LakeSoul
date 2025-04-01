package org.apache.spark.ml.lakesoul.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
  * Class for data normalization utilities
  */
object DataNormalizer {
  // Define normalization types
  val NORM_NONE = 0
  val NORM_MINMAX = 1
  val NORM_ZSCORE = 2

  def normalizationType(normType: String): Int = {
    normType match {
      case "NORM_NONE" => NORM_NONE
      case "NORM_MINMAX" => NORM_MINMAX
      case "NORM_ZSCORE" => NORM_ZSCORE
      case _ => throw new IllegalArgumentException(s"Unsupported normalization type: $normType")
    }
  }

  /**
    * Case class to hold normalization parameters
    */
  case class NormalizationParams(
                                  normTypeName: String,
                                  featureMin: Option[Array[Double]] = None,
                                  featureMax: Option[Array[Double]] = None,
                                  featureMeans: Option[Array[Double]] = None,
                                  featureStdDevs: Option[Array[Double]] = None,
                                  minMaxLower: Double = 0.0f,
                                  minMaxUpper: Double = 1.0f,
                                  zScoreTargetMean: Double = 0.0,
                                  zScoreTargetStdDev: Double = 1.0
                                )

  /**
    * Compute normalization parameters from training data
    *
    * @param trainVectors       Training data as Array of feature vectors
    * @param normType           Normalization type (NORM_NONE, NORM_MINMAX, NORM_ZSCORE)
    * @param zScoreTargetMean   Target mean for Z-Score normalization (default: 0.0)
    * @param zScoreTargetStdDev Target standard deviation for Z-Score normalization (default: 1.0)
    * @param minMaxLower        Lower bound for min-max normalization (default: 0.0)
    * @param minMaxUpper        Upper bound for min-max normalization (default: 1.0)
    * @return NormalizationParams containing the computed parameters
    */
  def computeNormalizationParams(
                                  trainVectors: Array[Array[Double]],
                                  normTypeName: String,
                                  minMaxLower: Double = 0.0f,
                                  minMaxUpper: Double = 1.0f,
                                  zScoreTargetMean: Double = 0.0,
                                  zScoreTargetStdDev: Double = 1.0): NormalizationParams = {

    val normType = normalizationType(normTypeName)

    // Print basic stats about the input data
    val numVectors = trainVectors.length
    val numFeatures = if (trainVectors.nonEmpty) trainVectors(0).length else 0
    println(s"Computing normalization parameters for $numVectors vectors with $numFeatures features")
    println(s"Normalization type: $normTypeName")

    normType match {
      case NORM_NONE =>
        // No parameters needed for no normalization
        println("No normalization will be applied")
        NormalizationParams(normTypeName = normTypeName)

      case NORM_MINMAX =>
        // Calculate global min-max normalization parameters
        val featureMin = Array.fill(numFeatures)(Double.MaxValue)
        val featureMax = Array.fill(numFeatures)(Double.MinValue)

        // Find min and max for each feature across all training vectors
        trainVectors.foreach { vec =>
          for (i <- 0 until numFeatures) {
            featureMin(i) = math.min(featureMin(i), vec(i))
            featureMax(i) = math.max(featureMax(i), vec(i))
          }
        }

        // Print distribution statistics for min-max normalization
        println("Min-Max Normalization Statistics:")
        println(s"Target range: [$minMaxLower, $minMaxUpper]")
        println("Feature\tMin\tMax\tRange")
        for (i <- 0 until math.min(numFeatures, 10)) { // Print first 10 features
          println(f"$i\t${featureMin(i)}%.4f\t${featureMax(i)}%.4f\t${featureMax(i) - featureMin(i)}%.4f")
        }
        if (numFeatures > 10) println("... (showing only first 10 features)")

        NormalizationParams(
          normTypeName = normTypeName,
          featureMin = Some(featureMin),
          featureMax = Some(featureMax),
          minMaxLower = minMaxLower,
          minMaxUpper = minMaxUpper
        )

      case NORM_ZSCORE =>
        // Calculate global normalization parameters for custom normal distribution
        val means = Array.fill(numFeatures)(0.0)
        
        // Step 1: Calculate means for each dimension
        trainVectors.foreach { vec =>
          for (i <- 0 until numFeatures) {
            means(i) += vec(i)
          }
        }

        // Divide sums by count to get means
        for (i <- 0 until numFeatures) {
          means(i) /= numVectors
        }

        // Step 2: Calculate variances for each dimension
        val variances = Array.fill(numFeatures)(0.0)
        trainVectors.foreach { vec =>
          for (i <- 0 until numFeatures) {
            val diff = vec(i) - means(i)
            variances(i) += diff * diff
          }
        }

        // Divide sum of squared differences by count to get variances
        for (i <- 0 until numFeatures) {
          variances(i) /= numVectors
        }

        // Step 3: Calculate standard deviations
        val stdDevs = variances.map(variance => math.sqrt(variance))

        // Print distribution statistics for z-score normalization
        println("Z-Score Normalization Statistics:")
        println(s"Target distribution: mean=$zScoreTargetMean, stdDev=$zScoreTargetStdDev")
        println("Feature\tMean\tStdDev\tMin\tMax")
        
        // Calculate min/max for reporting purposes
        val featureMin = Array.fill(numFeatures)(Double.MaxValue)
        val featureMax = Array.fill(numFeatures)(Double.MinValue)
        trainVectors.foreach { vec =>
          for (i <- 0 until numFeatures) {
            featureMin(i) = math.min(featureMin(i), vec(i))
            featureMax(i) = math.max(featureMax(i), vec(i))
          }
        }
        
        for (i <- 0 until math.min(numFeatures, 10)) { // Print first 10 features
          println(f"$i\t${means(i)}%.4f\t${stdDevs(i)}%.4f\t${featureMin(i)}%.4f\t${featureMax(i)}%.4f")
        }
        if (numFeatures > 10) println("... (showing only first 10 features)")

        NormalizationParams(
          normTypeName = normTypeName,
          featureMeans = Some(means),
          featureStdDevs = Some(stdDevs),
          zScoreTargetMean = zScoreTargetMean,
          zScoreTargetStdDev = zScoreTargetStdDev
        )

      case _ =>
        throw new IllegalArgumentException(s"Unsupported normalization type: $normType")
    }
  }

  /**
    * Apply normalization to data using pre-computed parameters
    *
    * @param vectors Data vectors to normalize
    * @param params  Normalization parameters computed by computeNormalizationParams
    * @return Normalized data as Array of feature vectors
    */
  def normalizeData(
                     vectors: Array[Array[Double]],
                     params: NormalizationParams): Array[Array[Double]] = {

    val normType = normalizationType(params.normTypeName)

    normType match {
      case NORM_NONE =>
        // Return original vectors without normalization
        vectors

      case NORM_MINMAX =>
        // Apply min-max normalization
        val featureMin = params.featureMin.getOrElse(
          throw new IllegalArgumentException("Missing min values for min-max normalization"))
        val featureMax = params.featureMax.getOrElse(
          throw new IllegalArgumentException("Missing max values for min-max normalization"))

        vectors.map { vec =>
          vec.zipWithIndex.map { case (value, i) =>
            if (featureMax(i) - featureMin(i) > 0.0001f) {
              (value - featureMin(i)) / (featureMax(i) - featureMin(i)) * (params.minMaxUpper - params.minMaxLower) + params.minMaxLower
            } else {
              params.minMaxLower // Handle case where max = min (constant feature)
            }
          }
        }

      case NORM_ZSCORE =>
        // Apply z-score normalization
        val means = params.featureMeans.getOrElse(
          throw new IllegalArgumentException("Missing means for z-score normalization"))
        val stdDevs = params.featureStdDevs.getOrElse(
          throw new IllegalArgumentException("Missing standard deviations for z-score normalization"))
        val targetMean = params.zScoreTargetMean
        val targetStdDev = params.zScoreTargetStdDev

        vectors.map { vec =>
          vec.zipWithIndex.map { case (value, i) =>
            if (stdDevs(i) > 0.0001) {
              // First convert to standard z-score (mean=0, stdDev=1),
              // then scale to target stdDev and shift to target mean
              ((value - means(i)) / stdDevs(i) * targetStdDev + targetMean)
            } else {
              targetMean // Handle case where stdDev is near zero (constant feature)
            }
          }
        }

      case _ =>
        throw new IllegalArgumentException(s"Unsupported normalization type: ${params.normTypeName}")
    }
  }
} 