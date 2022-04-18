/* Copyright (c) 2022 bitlap.org */
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
    fun Long.time(): DateTime {
        return DateTime(this)
    }

    /**
     * get utc time
     */
    fun Long.utc(): DateTime {
        return DateTime(this, DateTimeZone.UTC)
    }
}
