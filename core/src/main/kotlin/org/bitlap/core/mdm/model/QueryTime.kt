/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.mdm.model

import org.bitlap.common.TimeRange
import org.bitlap.common.utils.DateEx.utc

/**
 * Desc: Time for query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/19
 */
data class QueryTime(val timeRange: TimeRange) {

    constructor(start: Long) : this(TimeRange(start.utc()))
}
