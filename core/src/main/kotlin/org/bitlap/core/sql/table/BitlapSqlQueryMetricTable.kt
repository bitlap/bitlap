package org.bitlap.core.sql.table

import org.apache.calcite.DataContext
import org.apache.calcite.linq4j.Enumerable
import org.apache.calcite.linq4j.Linq4j
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.rex.RexNode
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.fetch
import org.bitlap.core.mdm.plan.MetricsPlan
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.MDColumnAnalyzer
import org.bitlap.core.sql.QueryContext

class BitlapSqlQueryMetricTable(
    override val table: Table,
    override val analyzer: MDColumnAnalyzer,
    private val timeFilter: RexNode,
    private val filters: RexNode,
) : BitlapSqlQueryTable(table) {

    // filters is empty here, pushed by BitlapFilterTableScanRule
    override fun scan(root: DataContext, filters: MutableList<RexNode>, projects: IntArray?): Enumerable<Array<Any?>> {
        return this.scan(root, projects)
    }

    fun scan(root: DataContext, projects: IntArray?): Enumerable<Array<Any?>> {
        val tbl = this.table
        val rowType = super.getRowType(root.typeFactory)
        val rexBuilder = RexBuilder(root.typeFactory)
        // check if time filter is always true
        val timeFilterFun = resolveTimeFilter(timeFilter, rowType, rexBuilder)
        if (timeFilterFun.invoke(-23333L)) {
            throw IllegalArgumentException("${Keyword.TIME} must be specified explicitly in where expression without always true.")
        }

//        val executor = RexExecutorImpl.getExecutable(
//            RexBuilder(root.typeFactory),
//            listOf(filters), rowType
//        )
//        val precondition = executor.function

        val materialize = analyzer.shouldMaterialize()
        val dimensions = analyzer.getQueryDimensionColNames()
        val metricCols = analyzer.getMetricColNames()
        val projections = projects!!.map { rowType.fieldList[it].name }

        @Suppress("UNCHECKED_CAST")
        return Linq4j.asEnumerable(
            fetch {
                runtimeConf = QueryContext.get().runtimeConf!!
                table = tbl
                plan = MetricsPlan(timeFilterFun, projections, dimensions, metricCols, materialize)
            }.asSequence().toList() as List<Array<Any?>>
        )
    }
}
