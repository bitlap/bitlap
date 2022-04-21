/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import org.apache.calcite.sql.SqlAggFunction
import org.apache.calcite.sql.`fun`.SqlCountAggFunction
import org.apache.calcite.sql.`fun`.SqlSumAggFunction
import org.bitlap.common.utils.PreConditions

/**
 * Analyse metric & dimension model from sql select node
 */
data class MDColumn(val name: String, val type: ColumnType) {

    var project: Boolean = false
    var filter: Boolean = false

    /**
     * if column is pure with no function call
     */
    var pure: Boolean = false

    /**
     * left: agg function
     * right: function quantifier, for example distinct and etc.
     */
    val aggregates = mutableSetOf<Pair<SqlAggFunction, String>>()

    fun checkType(targetType: ColumnType): MDColumn {
        PreConditions.checkExpression(
            type == targetType,
            msg = "Unable to judge Column $name target type: $targetType should equals to current type: $type"
        )
        return this
    }

    fun withFilter(f: Boolean) = this.also { it.filter = f }
    fun withProject(p: Boolean) = this.also { it.project = p }
    fun withPure(p: Boolean) = this.also { it.pure = p }
    fun withAgg(vararg agg: Pair<SqlAggFunction, String>) = this.also { it.aggregates.addAll(agg) }
    fun withAgg(agg: Set<Pair<SqlAggFunction, String>>) = this.also { it.aggregates.addAll(agg) }

    fun isDistinct(): Boolean {
        if (this.aggregates.isEmpty()) {
            return false
        }
        return this.aggregates.filter { it.first is SqlCountAggFunction }
            .any { it.second.lowercase() == "distinct" }
    }

    fun isSum(): Boolean {
        if (this.aggregates.isEmpty()) {
            return false
        }
        return this.aggregates.any { it.first is SqlSumAggFunction }
    }

    override fun toString(): String {
        return "MDColumn(name='$name', type=$type, project=$project, filter=$filter, pure=$pure, aggregates=$aggregates)"
    }
}

sealed interface ColumnType
object MetricCol : ColumnType
object DimensionCol : ColumnType
