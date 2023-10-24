/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.plan

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.{ FetchContext, FetchPlan }
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/** plan to fetch metrics
 */
class MetricFetchPlan(
  private val timeFilter: PruneTimeFilter,
  private val metrics: List[DataType],
  private val metricType: Class[_ <: DataType],
  private val dimension: String,
  private val pushedFilter: PrunePushedFilter,
  override val subPlans: List[FetchPlan] = List.empty[FetchPlan])
    extends FetchPlan {

  override def execute(context: FetchContext): RowIterator = {
    PreConditions.checkExpression(
      metrics.forall(_.getClass == metricType),
      "",
      s"MetricFetchPlan's metrics $metrics must have only one metric type ${metricType.getName}"
    )
    withFetcher(context) { fetcher =>
      dimension match {
        case null =>
          fetcher.fetchMetrics(
            context.table,
            timeFilter,
            metrics.map(_.name),
            metricType
          )
        case _ =>
          fetcher.fetchMetrics(
            context.table,
            timeFilter,
            metrics.map(_.name),
            metricType,
            dimension,
            pushedFilter
          )
      }
    }
  }

  override def explain(depth: Int): String = {
    s"${" ".repeat(depth)}+- MetricFetchPlan (metrics: $metrics, metricType: ${metricType.getSimpleName}, dimension: $dimension, timeFilter: $timeFilter, pushedFilter: $pushedFilter)"
  }
}
