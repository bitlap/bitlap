package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricRowMetaSimple

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
            val rows = LinkedHashMap<Long, MutableMap<String, MetricRowMetaSimple>>()
            fetcher.fetchMetricsMeta(context.table, timeFilter, metrics)
                .asSequence()
                .forEach {
                    val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                    if (row.containsKey(it.metricKey)) {
                        row[it.metricKey]!!.addEntityUniqueCount(it.entityUniqueCount).addEntityCount(it.entityCount)
                            .addMetricCount(it.metricCount)
                    } else {
                        row[it.metricKey] = MetricRowMetaSimple(it.entityUniqueCount, it.entityCount, it.metricCount)
                    }
                }
            rows.map { rs ->
                arrayOfNulls<Any>(projects.size).apply {
                    projects.mapIndexed { i, p ->
                        when (p) {
                            Keyword.TIME -> set(i, rs.key)
                            else -> set(i, rs.value[p] ?: MetricRowMetaSimple())
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
                    acc.forEachIndexed { i, e ->
                        val m1 = e as MetricRowMetaSimple
                        val m2 = an[i] as MetricRowMetaSimple
                        m1.addEntityUniqueCount(m2.entityUniqueCount).addEntityCount(m2.entityCount)
                            .addMetricCount(m2.metricCount)
                    }
                    acc
                }
                BitlapIterator.of(listOf(rs))
            }
        }
    }
}
