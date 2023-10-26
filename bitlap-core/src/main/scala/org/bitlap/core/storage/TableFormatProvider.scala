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
package org.bitlap.core.storage

import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta

import org.apache.hadoop.fs.Path

/** get table format implementation
 */
trait TableFormatProvider {
  def getMetricWriter(output: Path): BitlapWriter[MetricRow]
  def getMetricDimWriter(output: Path): BitlapWriter[MetricDimRow]

  def getMetricMetaReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String]
  ): BitlapReader[MetricRowMeta]

  def getMetricReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String]
  ): BitlapReader[MetricRow]

  def getMetricDimMetaReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapReader[MetricDimRowMeta]

  def getMetricDimReader(
    dataPath: Path,
    timeFunc: TimeFilterFun,
    metrics: List[String],
    projections: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapReader[MetricDimRow]
}
