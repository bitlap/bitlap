package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.DataContexts
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexExecutorImpl
import org.apache.calcite.rex.RexNode
import org.apache.calcite.sql.type.SqlTypeName
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.MDColumn
import org.bitlap.core.sql.MDColumnAnalyzer
import java.util.*

class BitlapSqlQueryMetricTable(
    override val table: Table,
    override val analyzer: MDColumnAnalyzer,
    val filters: List<RexNode>,
) : BitlapSqlQueryTable(table) {

    // filters is empty, pushed by BitlapFilterTableScanRule
    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        return this.scan(root, projects)
    }

    fun scan(root: DataContext, projects: IntArray?): Enumerable<Array<Any?>> {
        val rowType = super.getRowType(root.typeFactory)
        if (filters.isNotEmpty()) {
            val filter = filters.first()
            val executor = RexExecutorImpl.getExecutable(
                RexBuilder(root.typeFactory),
                filters, rowType
            )
            // why inputRecord? see DataContextInputGetter
            executor.setDataContext(DataContexts.of(mapOf("inputRecord" to arrayOf(0, 0, 123, "123"))))
            val aa = executor.execute()
            println(Arrays.toString(aa))
            executor.setDataContext(DataContexts.of(mapOf("inputRecord" to arrayOf(0, 0, 123, "1234"))))
            val aaa = executor.execute()
            println(Arrays.toString(aaa))
        }
        return Linq4j.asEnumerable(arrayOf(arrayOf(6, 3), arrayOf(6, 5)))
    }
}
