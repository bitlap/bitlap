package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.bitmap.BM

/**
 * compute count metric from bitmap or metadata.
 */
class UdafBMCountDistinct : UDAF<Number, Any, Number> {
    companion object {
        val NAME = "bm_count_distinct"
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlTypeName = SqlTypeName.BIGINT

    override fun init(): Number = 0L
    override fun add(accumulator: Number, input: Any): Number {
        return when (input) {
            is Array<*> -> {
                accumulator.toLong() + (input[0] as Number).toLong()
            }
            is BM -> {
                accumulator.toLong() + input.getCountUnique()
            }
            else -> {
                throw IllegalArgumentException("Invalid input type: ${input::class.java}")
            }
        }
    }

    override fun merge(accumulator1: Number, accumulator2: Number): Number =
        accumulator1.toLong() + accumulator2.toLong()

    override fun result(accumulator: Number): Number = accumulator
}
