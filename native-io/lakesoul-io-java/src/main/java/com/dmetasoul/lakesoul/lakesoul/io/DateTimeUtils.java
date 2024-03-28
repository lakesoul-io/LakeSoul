package com.dmetasoul.lakesoul.lakesoul.io;

import java.sql.Timestamp;
import java.time.*;


public class DateTimeUtils {
    private static final int MONTHS_PER_YEAR = 12;
    private static final byte DAYS_PER_WEEK = 7;
    private static final long HOURS_PER_DAY = 24L;
    private static final long MINUTES_PER_HOUR = 60L;
    private static final long SECONDS_PER_MINUTE = 60L;
    private static final long SECONDS_PER_HOUR = 3600L;
    private static final long SECONDS_PER_DAY = 86400L;
    private static final long MILLIS_PER_SECOND = 1000L;
    private static final long MILLIS_PER_MINUTE = 60000L;
    private static final long MILLIS_PER_HOUR = 3600000L;
    private static final long MILLIS_PER_DAY = 86400000L;
    private static final long MICROS_PER_MILLIS = 1000L;
    private static final long MICROS_PER_SECOND = 1000000L;
    private static final long MICROS_PER_MINUTE = 60000000L;
    private static final long MICROS_PER_HOUR = 3600000000L;
    private static final long MICROS_PER_DAY = 86400000000L;
    private static final long NANOS_PER_MICROS = 1000L;
    private static final long NANOS_PER_MILLIS = 1000000L;
    private static final long NANOS_PER_SECOND = 1000000000L;

    // See issue SPARK-35679
    // min second cause overflow in instant to micro
    private static final long MIN_SECONDS = Math.floorDiv(Long.MIN_VALUE, MICROS_PER_SECOND);

    public static Long toMicros(Object value) {
        if (value instanceof LocalDateTime) {
            LocalDateTime d = (LocalDateTime) value;
            value = d.toInstant(ZoneOffset.UTC);
            return instantToMicros((Instant) value);
        }
        if (value instanceof Timestamp) {
            return fromTimeStamp((Timestamp) value);
        }
        if (value instanceof Instant) {
            return instantToMicros((Instant) value);
        }
        return null;
    }

    static long fromTimeStamp(Timestamp t) {
        // not optimize
        return millisToMicros(t.getTime()) + (t.getNanos() / NANOS_PER_MICROS) % MICROS_PER_MILLIS;
    }

    static long millisToMicros(long millis) {
        return Math.multiplyExact(millis, MICROS_PER_MILLIS);
    }

    static long instantToMicros(Instant i) {
//        OffsetDateTime ot = Off
//        ZonedDateTime zt = i.atZone(ZoneId.of("Asia/Shanghai"));
//        i = zt.withZoneSameInstant(ZoneId.of("UTC")).toInstant();

//        i = Instant.ofEpochSecond(1612176000);
        long epochSecond = i.getEpochSecond();
        if (epochSecond == MIN_SECONDS) {
            long us = Math.multiplyExact(epochSecond + 1, MICROS_PER_SECOND);
            return Math.addExact(us, nanoToMicros(i.getNano()) - MICROS_PER_SECOND);
        }
        long us = Math.multiplyExact(epochSecond, MICROS_PER_SECOND);
        return Math.addExact(us, nanoToMicros(i.getNano()));
    }

    static long nanoToMicros(long duration) {
        long s = 1L;
        long m = Long.MAX_VALUE / 1000L;
        if (s <= 1000L) {
            return (s == 1000L) ? duration : duration / 1000L;
        } else if (duration > m) {
            return Long.MAX_VALUE;
        } else if (duration < -m) {
            return Long.MIN_VALUE;
        } else {
            return duration * 1000L;
        }
    }
}
