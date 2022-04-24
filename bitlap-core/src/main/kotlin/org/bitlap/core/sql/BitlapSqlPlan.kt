/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.sql

import org.apache.calcite.rel.RelNode

/**
 * sql plan
 */
data class BitlapSqlPlan(
    val statement: String,
    val rel: RelNode,
    val relOpt: RelNode, // optimized rel
) {

    fun explain(): String {
        return this.relOpt.explain()
    }
}
