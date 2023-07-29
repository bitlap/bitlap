/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.udf.aggr

import org.apache.calcite.sql.type.SqlReturnTypeInference
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.common.bitmap.CBM
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.infer
import org.bitlap.core.sql.udf.UDAF
import org.bitlap.core.sql.udf.UDFNames

/**
 * compute sum metric from bitmap or metadata.
 */
class BMSumAggr : UDAF<Number, Any, Number> {

    override val name: String = UDFNames.bm_sum_aggr
    override val inputTypes: List<SqlTypeName> = listOf(SqlTypeName.ANY)
    override val resultType: SqlReturnTypeInference = SqlTypeName.DOUBLE.infer()

    override fun init(): Number = 0.0
    override fun add(accumulator: Number, input: Any): Number {
        return when (input) {
            is RowValueMeta -> {
                accumulator.toDouble() + (input[2]).toDouble()
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
