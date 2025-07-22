package org.apache.spark.sql.execution.vectorized

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{CalendarIntervalType, DataTypes, DateType, DayTimeIntervalType, Decimal, DecimalType, TimestampNTZType, TimestampType, YearMonthIntervalType}
import org.apache.spark.unsafe.types.CalendarInterval

object VectorUtils {

  /**
   * Populate a column vector with values from an internal row.
   *
   * @param col the column vector to populate
   * @param row the internal row to read from
   * @param fieldIdx the index of the field in the internal row
   */
  def populate(col: WritableColumnVector, row: InternalRow, fieldIdx: Int): Unit = {
    val capacity = col.capacity
    val t = col.dataType
    if (row.isNullAt(fieldIdx)) col.putNulls(0, capacity)
    else if (t eq DataTypes.BooleanType) col.putBooleans(0, capacity, row.getBoolean(fieldIdx))
    else if (t eq DataTypes.BinaryType) col.putByteArray(0, row.getBinary(fieldIdx))
    else if (t eq DataTypes.ByteType) col.putBytes(0, capacity, row.getByte(fieldIdx))
    else if (t eq DataTypes.ShortType) col.putShorts(0, capacity, row.getShort(fieldIdx))
    else if (t eq DataTypes.IntegerType) col.putInts(0, capacity, row.getInt(fieldIdx))
    else if (t eq DataTypes.LongType) col.putLongs(0, capacity, row.getLong(fieldIdx))
    else if (t eq DataTypes.FloatType) col.putFloats(0, capacity, row.getFloat(fieldIdx))
    else if (t eq DataTypes.DoubleType) col.putDoubles(0, capacity, row.getDouble(fieldIdx))
    else if (t eq DataTypes.StringType) {
      val v = row.getUTF8String(fieldIdx)
      val bytes = v.getBytes
      for (i <- 0 until capacity) {
        col.putByteArray(i, bytes)
      }
    }
    else t match {
      case dt: DecimalType =>
        val d = row.getDecimal(fieldIdx, dt.precision, dt.scale)
        if (dt.precision <= Decimal.MAX_INT_DIGITS) col.putInts(0, capacity, d.toUnscaledLong.toInt)
        else if (dt.precision <= Decimal.MAX_LONG_DIGITS) col.putLongs(0, capacity, d.toUnscaledLong)
        else {
          val integer = d.toJavaBigDecimal.unscaledValue
          val bytes = integer.toByteArray
          for (i <- 0 until capacity) {
            col.putByteArray(i, bytes, 0, bytes.length)
          }
        }
      case _: CalendarIntervalType =>
        val c = row.get(fieldIdx, t).asInstanceOf[CalendarInterval]
        col.getChild(0).putInts(0, capacity, c.months)
        col.getChild(1).putInts(0, capacity, c.days)
        col.getChild(2).putLongs(0, capacity, c.microseconds)
      case _ => if (t.isInstanceOf[DateType] || t.isInstanceOf[YearMonthIntervalType]) col
        .putInts(0, capacity, row.getInt(fieldIdx))
      else if (t.isInstanceOf[TimestampType] || t.isInstanceOf[TimestampNTZType] || t
        .isInstanceOf[DayTimeIntervalType]) col.putLongs(0, capacity, row.getLong(fieldIdx))
      else throw new RuntimeException(String
        .format("DataType %s is not supported" + " in column vectorized reader.", t.sql))
    }
  }
}
