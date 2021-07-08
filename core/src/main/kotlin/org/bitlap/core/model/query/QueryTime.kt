package org.bitlap.core.model.query

import org.bitlap.common.TimeRange
import org.joda.time.DateTime

/**
 * Desc: Time for query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/19
 */
data class QueryTime(val timeRange: TimeRange) {

    constructor(start: Long) : this(TimeRange(DateTime(start)))

}
