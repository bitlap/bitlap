/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode
import org.bitlap.core.catalog.metadata.Table

/**
 * empty table
 */
class BitlapSqlQueryEmptyTable(override val table: Table) : BitlapSqlQueryTable(table) {

    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        return Linq4j.emptyEnumerable()
    }
}
