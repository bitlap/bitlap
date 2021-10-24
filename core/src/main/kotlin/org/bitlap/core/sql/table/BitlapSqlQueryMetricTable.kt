package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode
import org.bitlap.core.data.metadata.Table

class BitlapSqlQueryMetricTable(
    override val table: Table
) : BitlapSqlQueryTable(table) {

    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        return Linq4j.asEnumerable(arrayOf(arrayOf(6, "name1"), arrayOf(4, "name2")))
    }
}
