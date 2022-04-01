/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import java.io.Serializable

/**
 * Prune other filter to push down
 */
typealias PushedFilterFun = (String) -> Boolean

data class PrunePushedFilterExpr(
    val name: String,
    val op: FilterOp,
    val values: List<String>,
    val func: PushedFilterFun,
    val expr: String,
)

open class PrunePushedFilter : Serializable {

    private val conditions = mutableListOf<PrunePushedFilterExpr>()

    fun add(name: String, op: FilterOp, values: List<String>, func: PushedFilterFun, expr: String): PrunePushedFilter {
        return this.also { it.conditions.add(PrunePushedFilterExpr(name, op, values, func, expr)) }
    }

    fun filter(name: String?): PrunePushedFilter {
        val rs = this.conditions.filter { it.name == name }
        return PrunePushedFilter().also {
            it.conditions.addAll(rs)
        }
    }

    fun getNames(): Set<String> = this.conditions.map { it.name }.toSet()
    fun getConditions(): List<PrunePushedFilterExpr> = this.conditions

    override fun toString(): String {
        return this.conditions.map { it.expr }.toString()
    }
}
