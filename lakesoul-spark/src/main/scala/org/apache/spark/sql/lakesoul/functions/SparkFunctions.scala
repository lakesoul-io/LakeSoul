package org.apache.spark.sql.lakesoul.functions

import org.apache.spark.sql.connector.catalog.functions.UnboundFunction

import java.util.Locale

object SparkFunctions {

  private val unboundFunctions = Map(HammingDistFunc.name -> (new HammingDistFunc).asInstanceOf[UnboundFunction])

  def load(name: String): Option[UnboundFunction] = unboundFunctions.get(name.toLowerCase(Locale.ROOT))

  lazy val functionNames: Seq[String] = unboundFunctions.keys.toSeq
}
