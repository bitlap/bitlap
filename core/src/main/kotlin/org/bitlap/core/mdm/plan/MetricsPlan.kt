package org.bitlap.core.mdm.plan

import arrow.core.None
import arrow.core.Option
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.TimeFilterFun

/**
 * plan to fetch metrics
 */
class MetricsPlan(
    private val timeFilter: TimeFilterFun,
    private val metrics: List<DataType>,
    private val metricType: Class<out DataType>,
    private val dimension: Option<String> = None,
) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        PreConditions.checkExpression(
            metrics.all { it::class.java == metricType },
            msg = "MetricsPlan's metrics $metrics must have only one metric type ${metricType.name}"
        )
        return withFetcher(context) { fetcher ->
//            val x = fetcher.fetchMetrics(context.table, timeFilter, metrics.map { it.name }, "os" to listOf("Mac"), metricType)
            fetcher.fetchMetrics(context.table, timeFilter, metrics.map { it.name }, metricType)
        }
    }
}
