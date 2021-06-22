package org.bitlap.common.utils

import java.io.Serializable

/**
 * Desc: Range utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/6/17
 */
open class Range<C : Comparable<C>>(val lower: LeftCut<C>, val upper: RightCut<C>) : Serializable {

    constructor(lower: C, upper: C) : this(LeftCut(lower, BoundType.CLOSE), RightCut(upper, BoundType.CLOSE))

    companion object {

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
    }

    operator fun contains(value: C): Boolean {
        return lower <= value && upper >= value
    }

    fun isValid(): Boolean = lower <= upper
    fun isSingle(): Boolean = lower.compareTo(upper) == 0

    abstract class Cut<C : Comparable<C>>(open val endpoint: C?, open val boundType: BoundType) : Comparable<Cut<C>>, Serializable {

        abstract operator fun compareTo(other: C): Int

        override operator fun compareTo(other: Cut<C>): Int {
            if ((other is RightCut && other.boundType == BoundType.INFINITY) || (this is LeftCut && other.boundType == BoundType.INFINITY)) {
                return -1
            }
            if ((other is LeftCut && other.boundType == BoundType.INFINITY) || (this is RightCut && other.boundType == BoundType.INFINITY)) {
                return 1
            }
            return this.endpoint!!.compareTo(other.endpoint!!)
        }

        abstract fun <R: Comparable<R>> map(func: (C?) -> R?): Cut<R>
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

        override fun <R: Comparable<R>> map(func: (C?) -> R?): LeftCut<R> {
            return LeftCut(func(endpoint), boundType)
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

        override fun <R: Comparable<R>> map(func: (C?) -> R?): RightCut<R> {
            return RightCut(func(endpoint), boundType)
        }
    }

    enum class BoundType {
        OPEN, CLOSE, INFINITY
    }
}
