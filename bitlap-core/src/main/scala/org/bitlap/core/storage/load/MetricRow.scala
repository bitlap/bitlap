/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.load

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM

/** One row for metric data model.
 */
final case class MetricRow(
  tm: Long,
  override val metricKey: String,
  metric: CBM,
  entity: BBM,
  var metadata: MetricRowMeta)
    extends HasMetricKey {

  def this(
    tm: Long,
    metricKey: String,
    metric: CBM,
    entity: BBM
  ) = this(tm, metricKey, metric, entity, MetricRowMeta(tm, metricKey))
}

final case class MetricRowMeta(
  tm: Long,
  override val metricKey: String,
  entityUniqueCount: Long = 0,
  entityCount: Long = 0,
  metricCount: Double = 0.0)
    extends HasMetricKey
