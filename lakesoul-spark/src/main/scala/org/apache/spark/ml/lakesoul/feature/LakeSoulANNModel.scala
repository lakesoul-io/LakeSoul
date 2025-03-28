package org.apache.spark.ml.lakesoul.feature

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.ml.lakesoul.scanns.Types.{Item, ItemId}
import org.apache.spark.ml.lakesoul.scanns.algorithm.L2ScalarRandomProjectionNNS
import org.apache.spark.ml.lakesoul.scanns.model.LSHNearestNeighborSearchModel
import org.apache.spark.ml.lakesoul.scanns.params.{LSHNNSParams, ScalarRandomProjectionLSHNNSParams}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.Identifiable

import java.util.UUID

trait LakeSoulANNParams extends LSHNNSParams with ScalarRandomProjectionLSHNNSParams {

  final val sourceTable: Param[String] = new Param[String](this, "sourceTable", "LakeSoul table name, LakeSoulANNModel will be fit by the persisted data of this table")

  final def getSourceTable: String = $(sourceTable)


  val idColumnName: Param[String] = new Param[String](this, "idColumnName", "The column name of the id column from the source table")

  final def getIdColumnName: String = $(idColumnName)
  
  val algorithm: Param[String] = new Param[String](this, "algorithm", "The algorithm to use for the ANN model")

  final def getAlgorithm: String = $(algorithm)

  setDefault(sourceTable -> "default", idColumnName -> "id", algorithm -> "scalarRP")

  val numFeatures: IntParam = new IntParam(this, "numFeatures", "The number of features in the data")

  final def getNumFeatures: Int = $(numFeatures)
}

class LakeSoulANN(val spark: SparkSession, override val uid: String) extends LakeSoulANNParams {

  def this(spark: SparkSession) = this(spark, Identifiable.randomUID("lakesoul-ann"))

  def setNumFeatures(value: Int): this.type = super.set(numFeatures, value)

  /** @group setParam */
  def setSourceTable(value: String): this.type = super.set(sourceTable, value)

  /** @group setParam */
  def setIdColumnName(value: String): this.type = super.set(idColumnName, value)

  lazy val model: LSHNearestNeighborSearchModel[_] = createModel($(numFeatures))

  lazy val candidateRDD: RDD[Item] = {
    LakeSoulTable
      .forName(spark, $(sourceTable))
      .toDF
      .select($(idColumnName), "features")
      .rdd
      .map(row => (row.getAs[Long]($(idColumnName)), row.getAs[Vector]("features")))
  }


  def createModel(numFeatures: Int): LSHNearestNeighborSearchModel[_] = {
    $(algorithm) match {
      case "scalarRP" =>
        new L2ScalarRandomProjectionNNS()
          .setNumHashes($(numHashes))
          .setSignatureLength($(signatureLength))
          .setBucketWidth($(bucketWidth))
          .createModel(numFeatures)
      case _ => throw new IllegalArgumentException(s"Unsupported algorithm: ${$(algorithm)}")
    }
  }

  def getAllNearestNeighbors(query: RDD[Item], topK: Int): RDD[(ItemId, ItemId, Double)] = {

    model.getAllNearestNeighbors(query, candidateRDD, topK)
  }

  override def copy(extra: ParamMap): Params = defaultCopy(extra)

}