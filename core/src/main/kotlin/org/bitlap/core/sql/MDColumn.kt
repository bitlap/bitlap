package org.bitlap.core.sql

import org.apache.calcite.sql.SqlAggFunction
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

    val aggs = mutableSetOf<Pair<SqlAggFunction, String>>()

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
    fun withAgg(vararg agg: Pair<SqlAggFunction, String>) = this.also { it.aggs.addAll(agg) }
    fun withAgg(agg: Set<Pair<SqlAggFunction, String>>) = this.also { it.aggs.addAll(agg) }
}

sealed interface ColumnType
object MetricCol : ColumnType
object DimensionCol : ColumnType
