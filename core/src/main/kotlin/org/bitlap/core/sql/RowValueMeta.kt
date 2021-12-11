package org.bitlap.core.sql

import org.bitlap.common.utils.PreConditions

/**
 * wrapper a cell metric value
 *
 * 0: distinct count (Long)
 * 1: count (Long)
 * 2: sum (Double)
 */
class RowValueMeta {

    private val values: Array<Number> = arrayOf(0L, 0L, 0.0)

    companion object {
        fun empty() = RowValueMeta()
        fun of(v1: Long, v2: Long, v3: Double) = RowValueMeta().add0(v1).add1(v2).add2(v3)
    }

    fun add0(v: Number) = this.also { it.values[0] = v.toLong() + it.values[0].toLong() }
    fun add1(v: Number) = this.also { it.values[1] = v.toLong() + it.values[1].toLong() }
    fun add2(v: Number) = this.also { it.values[2] = v.toDouble() + it.values[2].toDouble() }

    operator fun component1(): Number = this.values[0]
    operator fun component2(): Number = this.values[1]
    operator fun component3(): Number = this.values[2]

    operator fun get(idx: Int): Number {
        PreConditions.checkExpression(idx in 0..2)
        return this.values[idx]
    }
}
