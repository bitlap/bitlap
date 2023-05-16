/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.rule

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`fun`.SqlAbstractGroupFunction
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlMinMaxAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.apache.calcite.sql.`fun`.SqlSumEmptyIsZeroAggFunction
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.sql.rel.BitlapAggregate
import org.bitlap.core.sql.udf.FunctionRegistry
import org.bitlap.core.sql.udf.UDFNames

/**
 * convert sum, count, count_distinct to internal agg type.
 */
class BitlapAggConverter : AbsRelRule(BitlapAggregate::class.java, "BitlapAggConverter") {

    companion object {
        // TODO (see AggregateReduceFunctionsRule, support other aggregate functions)
        private val NEED_CONVERTS = listOf(
            SqlSumAggFunction::class.java,
            SqlSumEmptyIsZeroAggFunction::class.java,
            SqlCountAggFunction::class.java,
            // SqlAvgAggFunction::class.java,
        )
    }

    override fun convert0(rel: RelNode, call: RelOptRuleCall): RelNode {
        rel as BitlapAggregate
        // 1. if it has no table scan, no need to convert
        // 2. if current is not first BitlapAggregate deeply, maybe sub query, no need to convert
        if (!this.hasTableScanNode(rel) || this.hasSecondAggregate(rel)) {
            return rel
        }
        val typeFactory = call.builder().typeFactory
        // check need converts
        val need = rel.aggCallList.any { NEED_CONVERTS.contains(it.aggregation::class.java) }
        if (!need) {
            return rel
        }

        // convert aggregate functions
        val aggCalls = rel.aggCallList.map {
            val aggFunc = it.aggregation
            var type = it.type
            var distinct = it.isDistinct
            val func = when (aggFunc) {
                is SqlSumAggFunction,
                is SqlSumEmptyIsZeroAggFunction -> {
                    type = typeFactory.createSqlType(SqlTypeName.DOUBLE)
                    FunctionRegistry.getFunction(UDFNames.bm_sum_aggr) as SqlAggFunction
                }
                is SqlCountAggFunction -> {
                    if (it.isDistinct) {
                        type = typeFactory.createSqlType(SqlTypeName.BIGINT)
                        distinct = false
                        FunctionRegistry.getFunction(UDFNames.bm_count_distinct) as SqlAggFunction
                    } else {
                        type = typeFactory.createSqlType(SqlTypeName.BIGINT)
                        FunctionRegistry.getFunction(UDFNames.bm_count_aggr) as SqlAggFunction
                    }
                }
                is SqlMinMaxAggFunction,
                is SqlAbstractGroupFunction -> {
                    aggFunc
                }
                else -> {
                    if (FunctionRegistry.contains(aggFunc.name)) {
                        aggFunc
                    } else {
                        throw IllegalArgumentException("${aggFunc.name} aggregate function is not supported.")
                    }
                }
            }
            AggregateCall.create(
                func, distinct, it.isApproximate, it.ignoreNulls(),
                it.argList, it.filterArg, it.distinctKeys,
                it.collation, type, it.name
            )
        }
        return rel.withAggCalls(aggCalls)
    }

    private fun hasSecondAggregate(rel: RelNode, inputs: Boolean = false): Boolean {
        if (rel is BitlapAggregate && inputs) {
            return true
        }
        return rel.inputs.mapNotNull { it.clean() }.any { this.hasSecondAggregate(it, true) }
    }
}
