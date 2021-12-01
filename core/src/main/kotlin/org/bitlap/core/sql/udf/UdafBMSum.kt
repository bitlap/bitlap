package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.bitmap.CBM

/**
 * compute sum metric from bitmap or metadata.
 */
class UdafBMSum : UDAF<Number, Any, Number> {
    companion object {
        val NAME = "bm_sum"
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlTypeName = SqlTypeName.DOUBLE

    override fun init(): Number = 0.0
    override fun add(accumulator: Number, input: Any): Number {
        return when (input) {
            is Array<*> -> {
                accumulator.toDouble() + (input[2] as Number).toDouble()
            }
            is CBM -> {
                accumulator.toDouble() + input.getCount()
            }
            else -> {
                throw IllegalArgumentException("Invalid input type: ${input::class.java}")
            }
        }
    }

    override fun merge(accumulator1: Number, accumulator2: Number): Number =
        accumulator1.toDouble() + accumulator2.toDouble()

    override fun result(accumulator: Number): Number = accumulator
}