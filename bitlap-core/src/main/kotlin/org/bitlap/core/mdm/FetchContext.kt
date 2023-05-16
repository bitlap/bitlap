/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm

import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.fetch.LocalFetcher
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.plan.MetricFetchPlan
import org.bitlap.core.mdm.plan.MetricMergeDimFetchPlan
import org.bitlap.core.mdm.plan.MetricMergeFetchPlan
import org.bitlap.core.mdm.plan.PendingFetchPlan
import org.bitlap.core.sql.MDColumnAnalyzer
import java.io.Serializable

/**
 * Fetch context wrapper
 */
class FetchContext(val table: Table, private val oPlan: FetchPlan) : Serializable {

    lateinit var bestPlan: FetchPlan

    /**
     * find best plan to be executed
     */
    fun findBestPlan(): FetchPlan {
        this.bestPlan = when (oPlan) {
            is PendingFetchPlan -> {
                val dimensions = oPlan.analyzer.getDimensionColNamesWithoutTime()
                when (dimensions.size) {
                    // one or no other dimension
                    0, 1 -> this.planWithNoOrOneOtherDim(dimensions.firstOrNull(), oPlan)

                    // two or more other dimensions
                    else -> this.planWithOtherDims(dimensions, oPlan)
                }
            }
            else -> oPlan
        }
        return this.bestPlan
    }

    /**
     * Merge metrics
     * 1. no other dimensions
     * 2. one other dimensions
     */
    private fun planWithNoOrOneOtherDim(dimension: String?, plan: PendingFetchPlan): FetchPlan {
        val metricParts = this.getMetricParts(plan.analyzer, true)
        val dimensionFilter = plan.pushedFilter.filter(dimension)
        return MetricMergeFetchPlan(
            subPlans = metricParts.map { e ->
                MetricFetchPlan(plan.timeFilter, e.value, e.key, dimension, dimensionFilter)
            }
        )
    }

    /**
     * Merge metrics with more than one dimensions
     *
     *
     * TODO should find best cartesian base here, use first one currently. For example:
     *   1. split into parts of a given size
     *   2. or split into parts with CBO
     *   3. or (merge metric -> mertic dim) or (merge dim -> merge metric)
     */
    private fun planWithOtherDims(dimensions: List<String>, plan: PendingFetchPlan): FetchPlan {
        val timeFilter = plan.timeFilter
        val pushedFilter = plan.pushedFilter

        val subPlans = dimensions.withIndex().chunked(2) { parts ->
            val ps = parts.map {
                val dimension = it.value
                val dimensionFilter = pushedFilter.filter(dimension)
                val metricParts = this.getMetricParts(plan.analyzer, it.index == 0)
                MetricMergeFetchPlan(
                    subPlans = metricParts.map { e ->
                        MetricFetchPlan(timeFilter, e.value, e.key, dimension, dimensionFilter)
                    }
                )
            }
            MetricMergeDimFetchPlan(ps)
        }
        return MetricMergeDimFetchPlan(subPlans)
    }

    private fun getMetricParts(analyzer: MDColumnAnalyzer, cartesianBase: Boolean): Map<Class<out DataType>, List<DataType>> {
        return analyzer.getMetricColNames()
            .map { analyzer.materializeType(it, cartesianBase) }
            .groupBy { it::class.java }
    }

    /**
     * find best data fetcher
     */
    fun findBestFetcher(plan: FetchPlan): Fetcher {
        return LocalFetcher(this)
    }
}
