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
package org.bitlap.core.mdm

import java.io.Serializable

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.fetch.LocalFetcher
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.plan.MetricFetchPlan
import org.bitlap.core.mdm.plan.MetricMergeDimFetchPlan
import org.bitlap.core.mdm.plan.MetricMergeFetchPlan
import org.bitlap.core.mdm.plan.PendingFetchPlan
import org.bitlap.core.sql.MDColumnAnalyzer

/** Fetch context wrapper
 */
class FetchContext(val table: Table, private val oPlan: FetchPlan) extends Serializable {

  var bestPlan: FetchPlan = _

  /** find best plan to be executed
   */
  def findBestPlan(): FetchPlan = {
    this.bestPlan = oPlan match {
      case p: PendingFetchPlan =>
        val dimensions = p.analyzer.getDimensionColNamesWithoutTime
        dimensions.size match {
          // one or no other dimension
          case 0 | 1 => this.planWithNoOrOneOtherDim(dimensions.headOption.orNull, p)

          // two or more other dimensions
          case _ => this.planWithOtherDims(dimensions, p)
        }
      case _ => oPlan
    }
    this.bestPlan
  }

  /** Merge metrics
   *    1. no other dimensions 2. one other dimensions
   */
  private def planWithNoOrOneOtherDim(dimension: String, plan: PendingFetchPlan): FetchPlan = {
    val metricParts     = this.getMetricParts(plan.analyzer, true)
    val dimensionFilter = plan.pushedFilter.filter(dimension)
    MetricMergeFetchPlan(
      metricParts.map { case (m, types) =>
        new MetricFetchPlan(plan.timeFilter, types, m, dimension, dimensionFilter)
      }.toList
    )
  }

  /** Merge metrics with more than one dimensions
   *
   *  TODO should find best cartesian base here, use first one currently. For example:
   *    - split into parts of a given size
   *    - or split into parts with CBO
   *    - or (merge metric -> mertic dim) or (merge dim -> merge metric)
   */
  private def planWithOtherDims(dimensions: List[String], plan: PendingFetchPlan): FetchPlan = {
    val timeFilter   = plan.timeFilter
    val pushedFilter = plan.pushedFilter

    val subPlans = dimensions.zipWithIndex.grouped(2).map { parts =>
      val ps = parts.map { it =>
        val dimension       = it._1
        val dimensionFilter = pushedFilter.filter(dimension)
        val metricParts     = this.getMetricParts(plan.analyzer, it._2 == 0)
        MetricMergeFetchPlan(
          metricParts.map { case (m, types) =>
            new MetricFetchPlan(timeFilter, types, m, dimension, dimensionFilter)
          }.toList
        )
      }
      MetricMergeDimFetchPlan(ps)
    }
    MetricMergeDimFetchPlan(subPlans.toList)
  }

  private def getMetricParts(analyzer: MDColumnAnalyzer, cartesianBase: Boolean)
    : Map[Class[_ <: DataType], List[DataType]] = {
    analyzer.getMetricColNames
      .map(it => analyzer.materializeType(it, cartesianBase))
      .groupBy(_.getClass)
  }

  /** find best data fetcher
   */
  def findBestFetcher(plan: FetchPlan): Fetcher = {
    LocalFetcher(this)
  }
}
