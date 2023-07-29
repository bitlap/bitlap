/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.aggr

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.BM
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDAF
import org.bitlap.core.sql.udf.UDFNames

/**
 * compute count metric from bitmap or metadata.
 */
class BMCountAggr : UDAF<Pair<Number, BM>, Any, Number> {

    override val name: String = UDFNames.bm_count_aggr
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlReturnTypeInference = SqlTypeName.BIGINT.infer()

    override fun init(): Pair<Number, BM> = 0L to BBM()
    override fun add(accumulator: Pair<Number, BM>, input: Any): Pair<Number, BM> {
        val (l, r) = accumulator
        return when (input) {
            is RowValueMeta -> {
                (l.toLong() + input[1].toLong()) to r
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
        val result = r.getCount().toLong()
        return if (result != 0L) result else l
    }
}
