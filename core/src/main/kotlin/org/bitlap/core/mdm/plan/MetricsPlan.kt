package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.RowValueMeta
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
            val rows = LinkedHashMap<Long, MutableMap<String, RowValueMeta>>()
            fetcher.fetchMetricsMeta(context.table, timeFilter, metrics)
                .asSequence()
                .forEach {
                    val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                    if (row.containsKey(it.metricKey)) {
                        row[it.metricKey]!!.add0(it.entityUniqueCount).add1(it.entityCount).add2(it.metricCount)
                    } else {
                        row[it.metricKey] = RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount)
                    }
                }
            rows.map { rs ->
                arrayOfNulls<Any>(projects.size).apply {
                    projects.mapIndexed { i, p ->
                        when (p) {
                            Keyword.TIME -> set(i, rs.key)
                            else -> set(i, rs.value[p] ?: RowValueMeta.empty())
                        }
                    }
                }
            }
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
