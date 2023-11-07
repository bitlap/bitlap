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
package org.bitlap.core.storage.load

import org.bitlap.roaringbitmap.x.BBM
import org.bitlap.roaringbitmap.x.CBM

/** One row for metric data model with one dimension.
 */
final case class MetricDimRow(
  tm: Long,
  override val metricKey: String,
  dimensionKey: String,
  dimension: String,
  metric: CBM,
  entity: BBM,
  var metadata: MetricDimRowMeta)
    extends HasMetricKey {

  def this(
    tm: Long,
    metricKey: String,
    dimensionKey: String,
    dimension: String,
    metric: CBM,
    entity: BBM
  ) = this(
    tm,
    metricKey,
    dimensionKey,
    dimension,
    metric,
    entity,
    MetricDimRowMeta(tm, metricKey, dimensionKey, dimension)
  )

}

final case class MetricDimRowMeta(
  tm: Long,
  override val metricKey: String,
  dimensionKey: String,
  dimension: String,
  entityUniqueCount: Long = 0,
  entityCount: Long = 0,
  metricCount: Double = 0.0)
    extends HasMetricKey
