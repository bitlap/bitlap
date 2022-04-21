/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.mdm.plan

import arrow.core.None
import arrow.core.Option
import arrow.core.Some
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/**
 * plan to fetch metrics
 */
class MetricsPlan(
    private val timeFilter: PruneTimeFilter,
    private val metrics: List<DataType>,
    private val metricType: Class<out DataType>,
    private val dimension: Option<String>,
    private val pushedFilter: PrunePushedFilter,
) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        PreConditions.checkExpression(
            metrics.all { it::class.java == metricType },
            msg = "MetricsPlan's metrics $metrics must have only one metric type ${metricType.name}"
        )
        return withFetcher(context) { fetcher ->
            when (dimension) {
                is None ->
                    fetcher.fetchMetrics(
                        context.table,
                        timeFilter, metrics.map { it.name }, metricType
                    )
                is Some ->
                    fetcher.fetchMetrics(
                        context.table,
                        timeFilter, metrics.map { it.name }, metricType,
                        dimension.value, pushedFilter
                    )
            }
        }
    }
}
