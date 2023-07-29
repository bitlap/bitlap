/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlNode

/**
 * sql plan
 */
data class BitlapSqlPlan(
    val statement: String,
    val sqlNode: SqlNode,
    val rel: RelNode,
    val relOpt: RelNode, // optimized rel
) {

    fun explain(): String {
        return this.relOpt.explain()
    }
}
