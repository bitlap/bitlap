package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.TimeFilterFun

/**
 * plan to fetch metrics
 */
class MetricsPlan(
    private val timeFilter: TimeFilterFun,
    private val projects: List<String>,
    private val dimensions: List<String>,
    private val metrics: List<String>,
    private val materialize: Boolean
) : AbsFetchPlan() {

    override fun execute(context: FetchContext): BitlapIterator<Array<*>> {
        PreConditions.checkExpression(dimensions.size <= 2)
        val hasTime = dimensions.contains(Keyword.TIME)
        val rawRows = withFetcher(context) { fetcher ->
            fetcher.fetchMetricsMeta(context.table, timeFilter, metrics)
                .transform(projects)
                .rows
                .asSequence().toList()
                .map { it.data }
        }

        @Suppress("UNCHECKED_CAST")
        return when {
            rawRows.isEmpty() -> BitlapIterator.empty()
            hasTime -> BitlapIterator.of(rawRows)
            else -> {
                val rs = rawRows.reduce { acc, an ->
                    for (i in acc.indices) {
                        val c1 = acc[i] as RowValueMeta
                        val (v0, v1, v2) = an[i] as RowValueMeta
                        c1.add0(v0).add1(v1).add2(v2)
                    }
                    acc
                }
                BitlapIterator.of(listOf(rs))
            }
        }
    }
}
