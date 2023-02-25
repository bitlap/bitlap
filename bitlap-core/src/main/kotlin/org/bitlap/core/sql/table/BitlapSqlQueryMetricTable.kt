/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexNode
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.fetch
import org.bitlap.core.mdm.plan.MetricsMergePlan
import org.bitlap.core.mdm.plan.MetricsPlan
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.sql.QueryContext

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
        val tbl = this.table
        val rowType = super.getRowType(root.typeFactory)

        val projections = projects.map { rowType.fieldList[it].name!! }
        val metricNames = analyzer.getMetricColNames()
        val dimension = analyzer.getDimensionColNamesWithoutTime()
            .firstOrNull() // only one or no dimension here
        val dimensionFilter = this.pushedFilter.filter(dimension)

        val metricParts = metricNames
            .map { analyzer.materializeType(it) }
            .groupBy { it::class.java }

        val rows = fetch {
            queryContext = QueryContext.get()
            table = tbl
            plan = MetricsMergePlan(
                subPlans = metricParts.map { e ->
                    MetricsPlan(timeFilter, e.value, e.key, dimension, dimensionFilter)
                }
            )
        }
        return Linq4j.asEnumerable(rows.toRows(projections))
    }
}
