package org.bitlap.core.mdm.model

import org.bitlap.common.utils.PreConditions
import java.io.Serializable

/**
 * wrapper a cell metric value
 *
 * * 0: distinct count (Long)
 * * 1: count (Long)
 * * 2: sum (Double)
 */
open class RowValueMeta : Serializable {

    private val values: Array<Number> = arrayOf(0L, 0L, 0.0)

    companion object {
        fun empty() = RowValueMeta()
        fun of(v0: Long, v1: Long, v2: Double) = RowValueMeta().add0(v0).add1(v1).add2(v2)
    }

    fun add0(v: Number) = this.also { it.values[0] = v.toLong() + it.values[0].toLong() }
    fun add1(v: Number) = this.also { it.values[1] = v.toLong() + it.values[1].toLong() }
    fun add2(v: Number) = this.also { it.values[2] = v.toDouble() + it.values[2].toDouble() }
    fun add(v0: Number, v1: Number, v2: Number) = this.add0(v0).add1(v1).add2(v2)
    fun add(v: RowValueMeta) = this.add0(v.component1()).add1(v.component2()).add2(v.component3())

    operator fun component1(): Number = this.values[0]
    operator fun component2(): Number = this.values[1]
    operator fun component3(): Number = this.values[2]

    operator fun get(idx: Int): Number {
        PreConditions.checkExpression(idx in 0..2)
        return this.values[idx]
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as RowValueMeta
        if (!values.contentEquals(other.values)) return false
        return true
    }

    override fun hashCode(): Int {
        return values.contentHashCode()
    }

    override fun toString(): String {
        val (v0, v1, v2) = this
        return "[v0=$v0, v1=$v1, v2=$v2]"
    }
}
