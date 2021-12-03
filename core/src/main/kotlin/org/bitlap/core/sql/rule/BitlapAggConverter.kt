package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.sql.udf.UdafBMCountDistinct
import org.bitlap.core.sql.udf.UdafBMSum

/**
 * convert sum, count, count_distinct to internal agg type.
 */
class BitlapAggConverter : AbsRelRule(BitlapAggregate::class.java, "BitlapAggConverter") {

    companion object {
        private val NEED_CONVERTS = listOf(
            SqlSumAggFunction::class.java,
            SqlSumEmptyIsZeroAggFunction::class.java,
            SqlCountAggFunction::class.java,
            // SqlAvgAggFunction::class.java,
        )
    }

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        val typeFactory = call.builder().typeFactory
        rel as BitlapAggregate
        // check need converts
        val need = rel.aggCallList.any { NEED_CONVERTS.contains(it.aggregation::class.java) }
        if (!need) {
            return rel
        }

        // convert aggregate functions
        val aggCalls = rel.aggCallList.map {
            val aggFunc = it.aggregation
            var type = it.type
            val func = when (aggFunc) {
                is SqlSumAggFunction,
                is SqlSumEmptyIsZeroAggFunction -> {
                    type = typeFactory.createSqlType(SqlTypeName.DOUBLE)
                    FunctionRegistry.getFunction(UdafBMSum.NAME) as SqlAggFunction
                }
                is SqlCountAggFunction -> {
                    if (it.isDistinct) {
                        type = typeFactory.createSqlType(SqlTypeName.BIGINT)
                        FunctionRegistry.getFunction(UdafBMCountDistinct.NAME) as SqlAggFunction
                    } else {
                        aggFunc
                    }
                }
                else -> {
                    if (FunctionRegistry.contanis(aggFunc.name)) {
                        aggFunc
                    } else {
                        throw IllegalArgumentException("${aggFunc.name} aggregate function is not supported.")
                    }
                }
            }
            AggregateCall.create(
                func, it.isDistinct, it.isApproximate, it.ignoreNulls(),
                it.argList, it.filterArg, it.distinctKeys,
                it.collation, type, it.name
            )
        }
        return rel.withAggCalls(aggCalls)
    }
}
