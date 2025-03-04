package org.apache.spark.sql.lakesoul.functions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, LongType, StructType}

class HammingDistFunc extends UnboundFunction {

  override def bind(structType: StructType): BoundFunction = {
    if (structType.fields.length != 2) {
      throw new UnsupportedOperationException(s"Cannot bind: ${this.name()} does not accept arguments type $structType")
    }
    if (!structType.fields.forall(f => checkFieldType(f.dataType))) {
      throw new UnsupportedOperationException(s"Cannot bind: ${this.name()} does not accept arguments type $structType")
    }
    new HammingDistFuncImpl
  }

  override def description(): String = "Returns hamming distance of two embedding vector"

  override def name(): String = HammingDistFunc.name

  private def checkFieldType(d: DataType) = {
    d match {
      case ArrayType(a, _) if a.isInstanceOf[LongType] => true
      case _ => false
    }
  }

  private class HammingDistFuncImpl extends ScalarFunction[Int] {

    override def inputTypes(): Array[DataType] = Array(ArrayType(LongType), ArrayType(LongType))

    override def resultType(): DataType = IntegerType

    override def name(): String = HammingDistFunc.name

    override def produceResult(input: InternalRow): Int = {
      val a0 = input.getArray(0).toLongArray
      val a1 = input.getArray(1).toLongArray
      require (a0.length == a1.length, "The input sequences must have the same length")
      a0.zip(a1).map { zipped =>
        java.lang.Long.bitCount(zipped._1 ^ zipped._2)
      }.sum
    }
  }
}

object HammingDistFunc {
  val name = "lakesoul_hamming_distance"
}