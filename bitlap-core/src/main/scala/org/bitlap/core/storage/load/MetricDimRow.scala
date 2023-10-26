/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.load

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM

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
