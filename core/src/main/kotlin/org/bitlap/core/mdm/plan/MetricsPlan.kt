package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.sql.TimeFilterFun

/**
 * plan to fetch metrics
 */
class MetricsPlan(
    private val timeFilter: TimeFilterFun,
    private val metrics: List<String>,
    private val materialize: Boolean
) : AbsFetchPlan() {

    constructor(timeFilter: TimeFilterFun, metric: String, materialize: Boolean) :
        this(timeFilter, listOf(metric), materialize)

    override fun execute(context: FetchContext): BitlapIterator<Array<*>> {
        val table = context.table
        return withFetcher(context) { fetcher ->
            val metas = fetcher.fetchMetricsMeta(table, timeFilter, metrics).asSequence()
            val rowMap = metas.groupBy { it.metricKey }
                .mapValues { rs ->
                    val row = arrayOf(0.0, 0.0, 0.0)
                    rs.value.forEach {
                        row[0] = row[0] + it.entityUniqueCount
                        row[1] = row[1] + it.entityCount
                        row[2] = row[2] + it.metricCount
                    }
                    row
                }
            if (rowMap.isEmpty()) {
                BitlapIterator.empty()
            } else {
                val row = metrics.map { rowMap[it] ?: arrayOf(0.0, 0.0, 0.0) }.toTypedArray()
                BitlapIterator.of(listOf(row))
            }
        }
    }
}
