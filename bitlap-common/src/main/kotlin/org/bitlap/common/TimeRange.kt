/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common

import org.bitlap.common.utils.Range
import org.joda.time.DateTime

/**
 * Desc: Time range utils, startTime and endTime must be specified.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/6/21
 */
class TimeRange private constructor(lower: LeftCut<DateTime>, upper: RightCut<DateTime>) : Range<DateTime>(lower, upper) {

    constructor(lower: DateTime, upper: DateTime) : this(LeftCut(lower, BoundType.CLOSE), RightCut(upper, BoundType.CLOSE))
    constructor(time: DateTime) : this(time, time)

    val startTime = lower.endpoint!!
    val endTime = upper.endpoint!!

    companion object {
        fun of(start: DateTime, end: DateTime, inclusive: Pair<Boolean, Boolean> = true to true): TimeRange {
            return when (inclusive) {
                true to true -> TimeRange(LeftCut(start, BoundType.CLOSE), RightCut(end, BoundType.CLOSE))
                true to false -> TimeRange(LeftCut(start, BoundType.CLOSE), RightCut(end, BoundType.OPEN))
                false to true -> TimeRange(LeftCut(start, BoundType.CLOSE), RightCut(end, BoundType.OPEN))
                false to false -> TimeRange(LeftCut(start, BoundType.OPEN), RightCut(end, BoundType.OPEN))
                else -> TimeRange(start, end)
            }
        }
    }

    operator fun component1(): DateTime = lower.endpoint!!
    operator fun component2(): DateTime = upper.endpoint!!

    fun <R> walkByDayStep(func: (DateTime) -> R): List<R> {
        var walkStart = startTime.withTimeAtStartOfDay().let {
            if (it.isBefore(startTime) || lower.boundType == BoundType.CLOSE) {
                it
            } else {
                it.plusDays(1)
            }
        }
        val walkEnd = endTime.withTimeAtStartOfDay().let {
            if (it.isEqual(endTime) && upper.boundType == BoundType.CLOSE) {
                it
            } else {
                it.plusDays(1)
            }
        }
        val results = mutableListOf<R>()
        while (walkStart.isBefore(walkEnd) || walkStart.isEqual(walkEnd)) {
            results.add(func.invoke(walkStart))
            walkStart = walkStart.plusDays(1)
        }
        return results
    }
}
