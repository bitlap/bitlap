/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common.utils

import java.io.Serializable

/**
 * Desc: Range utils, reference to guava Range.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/6/17
 */
open class Range<C : Comparable<C>>(val lower: LeftCut<C>, val upper: RightCut<C>) : Serializable {

    constructor(lower: C, upper: C) : this(LeftCut(lower, BoundType.CLOSE), RightCut(upper, BoundType.CLOSE))

    companion object {

        /**
         * [[endpoint], [endpoint]]
         */
        fun <C : Comparable<C>> singleton(endpoint: C): Range<C> {
            return Range(LeftCut(endpoint, BoundType.CLOSE), RightCut(endpoint, BoundType.CLOSE))
        }

        /**
         * ([lower], [upper])
         */
        fun <C : Comparable<C>> open(lower: C, upper: C): Range<C> {
            return Range(LeftCut(lower, BoundType.OPEN), RightCut(upper, BoundType.OPEN))
        }

        /**
         * [[lower], [upper]]
         */
        fun <C : Comparable<C>> closed(lower: C, upper: C): Range<C> {
            return Range(LeftCut(lower, BoundType.CLOSE), RightCut(upper, BoundType.CLOSE))
        }

        /**
         * ([lower], [upper]]
         */
        fun <C : Comparable<C>> openClosed(lower: C, upper: C): Range<C> {
            return Range(LeftCut(lower, BoundType.OPEN), RightCut(upper, BoundType.CLOSE))
        }

        /**
         * [[lower], [upper])
         */
        fun <C : Comparable<C>> closedOpen(lower: C, upper: C): Range<C> {
            return Range(LeftCut(lower, BoundType.CLOSE), RightCut(upper, BoundType.OPEN))
        }

        /**
         * (-∞, [upper])
         */
        fun <C : Comparable<C>> lessThan(upper: C): Range<C> {
            return Range(LeftCut(null, BoundType.INFINITY), RightCut(upper, BoundType.OPEN))
        }

        /**
         * (-∞, [upper]]
         */
        fun <C : Comparable<C>> atMost(upper: C): Range<C> {
            return Range(LeftCut(null, BoundType.INFINITY), RightCut(upper, BoundType.CLOSE))
        }

        /**
         * ([upper], +∞)
         */
        fun <C : Comparable<C>> greaterThan(lower: C): Range<C> {
            return Range(LeftCut(lower, BoundType.OPEN), RightCut(null, BoundType.INFINITY))
        }

        /**
         * [[upper], +∞)
         */
        fun <C : Comparable<C>> atLeast(lower: C): Range<C> {
            return Range(LeftCut(lower, BoundType.CLOSE), RightCut(null, BoundType.INFINITY))
        }

        /**
         * (-∞, +∞)
         */
        fun <C : Comparable<C>> all(): Range<C> {
            return Range(LeftCut(null, BoundType.INFINITY), RightCut(null, BoundType.INFINITY))
        }
    }

    operator fun contains(value: C): Boolean {
        return lower <= value && upper >= value
    }

    // TODO: containsAll, encloses, intersection...

    fun isValid(): Boolean = lower <= upper
    fun isEmpty(): Boolean = lower > upper ||
        (lower.compareTo(upper) == 0 && (lower.boundType == BoundType.OPEN || upper.boundType == BoundType.OPEN))

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Range<*>
        if (lower != other.lower) return false
        if (upper != other.upper) return false
        return true
    }

    override fun hashCode(): Int {
        var result = lower.hashCode()
        result = 31 * result + upper.hashCode()
        return result
    }

    override fun toString(): String {
        return "${lower.describe()}..${upper.describe()}"
    }

    abstract class Cut<C : Comparable<C>>(open val endpoint: C?, open val boundType: BoundType) : Comparable<Cut<C>>, Serializable {

        abstract operator fun compareTo(other: C): Int

        override operator fun compareTo(other: Cut<C>): Int {
            if ((this is LeftCut && this.isInfinity()) || (other is RightCut && other.isInfinity())) {
                return -1
            }
            if ((other is LeftCut && other.isInfinity()) || (this is RightCut && this.isInfinity())) {
                return 1
            }
            return this.endpoint!!.compareTo(other.endpoint!!)
        }

        protected fun isInfinity(): Boolean {
            return endpoint == null || boundType == BoundType.INFINITY
        }

        abstract fun <R : Comparable<R>> map(func: (C?) -> R?): Cut<R>
        abstract fun describe(): String
    }

    data class LeftCut<C : Comparable<C>>(override val endpoint: C?, override val boundType: BoundType) : Cut<C>(endpoint, boundType) {

        override operator fun compareTo(other: C): Int {
            if (endpoint == null) {
                return -1
            }
            return when (boundType) {
                BoundType.INFINITY -> -1
                BoundType.OPEN -> endpoint.compareTo(other) + 1
                BoundType.CLOSE -> endpoint.compareTo(other)
            }
        }

        override fun <R : Comparable<R>> map(func: (C?) -> R?): LeftCut<R> {
            return LeftCut(func(endpoint), boundType)
        }

        override fun describe(): String {
            if (endpoint == null || boundType == BoundType.INFINITY) {
                return "(-\u221e"
            }
            return when (boundType) {
                BoundType.OPEN -> "($endpoint"
                BoundType.CLOSE -> "[$endpoint"
                else -> ""
            }
        }
    }

    data class RightCut<C : Comparable<C>>(override val endpoint: C?, override val boundType: BoundType) : Cut<C>(endpoint, boundType) {

        override operator fun compareTo(other: C): Int {
            if (endpoint == null) {
                return 1
            }
            return when (boundType) {
                BoundType.INFINITY -> 1
                BoundType.OPEN -> endpoint.compareTo(other) - 1
                BoundType.CLOSE -> endpoint.compareTo(other)
            }
        }

        override fun <R : Comparable<R>> map(func: (C?) -> R?): RightCut<R> {
            return RightCut(func(endpoint), boundType)
        }

        override fun describe(): String {
            if (endpoint == null || boundType == BoundType.INFINITY) {
                return "+\u221e)"
            }
            return when (boundType) {
                BoundType.OPEN -> "$endpoint)"
                BoundType.CLOSE -> "$endpoint]"
                else -> ""
            }
        }
    }

    enum class BoundType {
        OPEN, CLOSE, INFINITY
    }
}
