package org.bitlap.core.sql

import java.io.Serializable

/**
 * Prune time filter to push down
 */
typealias TimeFilterFun = (Long) -> Boolean

data class PruneTimeFilterExpr(val name: String, val func: TimeFilterFun, val expr: String)

open class PruneTimeFilter : Serializable {

    private val conditions = mutableListOf<PruneTimeFilterExpr>()

    fun add(name: String, func: TimeFilterFun, expr: String): PruneTimeFilter {
        return this.also { it.conditions.add(PruneTimeFilterExpr(name, func, expr)) }
    }

    /**
     * merge conditions into one
     */
    fun mergeCondition(): TimeFilterFun {
        return { i ->
            this.conditions.map { it.func }.all { it(i) }
        }
    }

    override fun toString(): String {
        return this.conditions.map { it.expr }.toString()
    }
}
