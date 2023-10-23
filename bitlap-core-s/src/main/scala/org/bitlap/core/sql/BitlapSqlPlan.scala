/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql

import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.SqlNode

/**
 * sql plan
 */
case class BitlapSqlPlan(
    val statement: String,
    val sqlNode: SqlNode,
    val rel: RelNode,
    val relOpt: RelNode // optimized rel
) {

    def explain(): String = {
        return this.relOpt.explain()
    }
}
