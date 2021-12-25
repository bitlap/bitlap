package org.bitlap.core.sql.udf

import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.bitmap.BM
import org.bitlap.common.bitmap.RBM
import org.bitlap.core.mdm.model.RowValueMeta

/**
 * compute count metric from bitmap or metadata.
 */
class UdafBMCountDistinct : UDAF<Pair<Number, BM>, Any, Number> {
    companion object {
        const val NAME = "bm_count_distinct"
    }

    override val name: String = NAME
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlTypeName = SqlTypeName.BIGINT

    override fun init(): Pair<Number, BM> = 0L to RBM()
    override fun add(accumulator: Pair<Number, BM>, input: Any): Pair<Number, BM> {
        val (l, r) = accumulator
        return when (input) {
            is RowValueMeta -> {
                (l.toLong() + input[0].toLong()) to r
            }
            is BM -> {
                l to r.or(input)
            }
            else -> {
                throw IllegalArgumentException("Invalid input type: ${input::class.java}")
            }
        }
    }

    override fun merge(accumulator1: Pair<Number, BM>, accumulator2: Pair<Number, BM>): Pair<Number, BM> {
        val (l1, r1) = accumulator1
        val (l2, r2) = accumulator2
        return (l1.toLong() + l2.toLong()) to (r1.or(r2))
    }

    override fun result(accumulator: Pair<Number, BM>): Number {
        val (l, r) = accumulator
        val result = r.getCountUnique()
        return if (result != 0L) result else l
    }
}
