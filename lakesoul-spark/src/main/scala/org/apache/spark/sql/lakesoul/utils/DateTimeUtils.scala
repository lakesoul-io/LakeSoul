// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.utils

import java.sql.Timestamp
import java.time._
import java.util.TimeZone
import java.util.concurrent.TimeUnit._

/**
  * Forked from [[org.apache.spark.sql.catalyst.util.DateTimeUtils]].
  * Only included the methods that are used by LakeSoulTableRel and added after Spark 2.4.
  */

/**
  * Helper functions for converting between internal and external date and time representations.
  * Dates are exposed externally as java.sql.Date and are represented internally as the number of
  * dates since the Unix epoch (1970-01-01). Timestamps are exposed externally as java.sql.Timestamp
  * and are stored internally as longs, which are capable of storing timestamps with microsecond
  * precision.
  */
object DateTimeUtils {

  // we use Int and Long internally to represent [[DateType]] and [[TimestampType]]
  type SQLDate = Int
  type SQLTimestamp = Long

  // Pre-calculated values can provide an opportunity of additional optimizations
  // to the compiler like constants propagation and folding.
  final val NANOS_PER_MICROS: Long = 1000
  final val MICROS_PER_MILLIS: Long = 1000
  final val MILLIS_PER_SECOND: Long = 1000
  final val SECONDS_PER_DAY: Long = 24 * 60 * 60
  final val MICROS_PER_SECOND: Long = MILLIS_PER_SECOND * MICROS_PER_MILLIS
  final val NANOS_PER_MILLIS: Long = NANOS_PER_MICROS * MICROS_PER_MILLIS
  final val NANOS_PER_SECOND: Long = NANOS_PER_MICROS * MICROS_PER_SECOND
  final val MICROS_PER_DAY: Long = SECONDS_PER_DAY * MICROS_PER_SECOND
  final val MILLIS_PER_MINUTE: Long = 60 * MILLIS_PER_SECOND
  final val MILLIS_PER_HOUR: Long = 60 * MILLIS_PER_MINUTE
  final val MILLIS_PER_DAY: Long = SECONDS_PER_DAY * MILLIS_PER_SECOND

  def defaultTimeZone(): TimeZone = TimeZone.getDefault()

  def getTimeZone(timeZoneId: String): TimeZone = {
    val zoneId = ZoneId.of(timeZoneId, ZoneId.SHORT_IDS)
    TimeZone.getTimeZone(zoneId)
  }

  // Converts Timestamp to string according to Hive TimestampWritable convention.
  def timestampToString(tf: TimestampFormatter, us: SQLTimestamp): String = {
    tf.format(us)
  }

  def instantToMicros(instant: Instant): Long = {
    val us = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    result
  }

  def microsToInstant(us: Long): Instant = {
    val secs = Math.floorDiv(us, MICROS_PER_SECOND)
    val mos = Math.floorMod(us, MICROS_PER_SECOND)
    Instant.ofEpochSecond(secs, mos * NANOS_PER_MICROS)
  }

  def instantToDays(instant: Instant): Int = {
    val seconds = instant.getEpochSecond
    val days = Math.floorDiv(seconds, SECONDS_PER_DAY)
    days.toInt
  }

  /**
    * Returns the number of micros since epoch from java.sql.Timestamp.
    */
  def fromJavaTimestamp(t: Timestamp): SQLTimestamp = {
    if (t != null) {
      MILLISECONDS.toMicros(t.getTime()) + NANOSECONDS.toMicros(t.getNanos()) % NANOS_PER_MICROS
    } else {
      0L
    }
  }
}
