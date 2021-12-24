package org.bitlap.core.mdm.plan

import arrow.core.None
import arrow.core.Option
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.TimeFilterFun

/**
 * plan to fetch metrics
 */
class MetricsPlan(
    private val timeFilter: TimeFilterFun,
    private val materialize: Boolean,
    private val metrics: List<String>,
    private val dimension: Option<String> = None,
) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        return withFetcher(context) { fetcher ->
            if (materialize) {
                fetcher.fetchMetrics(context.table, timeFilter, metrics)
            } else {
                fetcher.fetchMetricsMeta(context.table, timeFilter, metrics)
            }
        }
    }
}
