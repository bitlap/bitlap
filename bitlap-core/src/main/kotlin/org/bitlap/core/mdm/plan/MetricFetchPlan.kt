/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.plan

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/**
 * plan to fetch metrics
 */
class MetricFetchPlan(
    private val timeFilter: PruneTimeFilter,
    private val metrics: List<DataType>,
    private val metricType: Class<out DataType>,
    private val dimension: String?,
    private val pushedFilter: PrunePushedFilter,
) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        PreConditions.checkExpression(
            metrics.all { it::class.java == metricType },
            msg = "MetricFetchPlan's metrics $metrics must have only one metric type ${metricType.name}"
        )
        return withFetcher(context) { fetcher ->
            when (dimension) {
                null ->
                    fetcher.fetchMetrics(
                        context.table,
                        timeFilter, metrics.map { it.name }, metricType
                    )
                else ->
                    fetcher.fetchMetrics(
                        context.table,
                        timeFilter, metrics.map { it.name }, metricType,
                        dimension, pushedFilter
                    )
            }
        }
    }

    override fun explain(depth: Int): String {
        return "${" ".repeat(depth)}+- MetricFetchPlan (metrics: $metrics, metricType: ${metricType.simpleName}, dimension: $dimension, timeFilter: $timeFilter, pushedFilter: $pushedFilter)"
    }
}
