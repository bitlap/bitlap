/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.plan.PendingFetchPlan
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

class BitlapSqlQueryMetricTable(
    override val table: Table,
    override val analyzer: MDColumnAnalyzer,
    private val timeFilter: PruneTimeFilter,
    private val pushedFilter: PrunePushedFilter,
) : BitlapSqlQueryTable(table) {

    // filters is empty here, pushed by BitlapFilterTableScanRule
    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        if (projects == null) {
            return Linq4j.emptyEnumerable()
        }
        return this.scan(root, projects)
    }

    private fun scan(root: DataContext, projects: IntArray): Enumerable<Array<Any?>> {
        val rowType = super.getRowType(root.typeFactory)
        val projections = projects.map { rowType.fieldList[it].name!! }

        // add pending fetch plan to context
        val fetchContext = FetchContext(table, PendingFetchPlan(table, analyzer, timeFilter, pushedFilter))

        // find best plan to execute
        val bestPlan = fetchContext.findBestPlan()

        // execute the plan
        val rows = bestPlan.execute(fetchContext)

        return Linq4j.asEnumerable(rows.toRows(projections))
    }
}
