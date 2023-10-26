/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.common.utils

import java.io.Serializable

/**
 * Range utils, reference to guava Range.
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

    // TODO (containsAll, encloses, intersection...)

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
