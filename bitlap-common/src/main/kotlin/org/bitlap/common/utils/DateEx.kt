/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.utils

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

/**
 * Date functions
 */
object DateEx {

    /**
     * get time with local zone
     */
    @JvmStatic
    @JvmOverloads
    fun Long.time(zone: DateTimeZone = DateTimeZone.getDefault()): DateTime {
        return DateTime(this, zone)
    }

    /**
     * get time with local zone
     */
    @JvmStatic
    @JvmOverloads
    fun String.time(zone: DateTimeZone = DateTimeZone.getDefault()): DateTime {
        return DateTime(this, zone)
    }

    /**
     * get utc time
     */
    @JvmStatic
    fun Long.utc(): DateTime {
        return DateTime(this, DateTimeZone.UTC)
    }
}
