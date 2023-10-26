/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
