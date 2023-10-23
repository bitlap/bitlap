/** Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.load

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM

/** Desc: One row for metric data model.
 */
case class MetricRow(
  val tm: Long,
  override val metricKey: String,
  val metric: CBM,
  val entity: BBM,
  var metadata: MetricRowMeta)
    extends HasMetricKey {

  def this(
    tm: Long,
    metricKey: String,
    metric: CBM,
    entity: BBM
  ) = this(tm, metricKey, metric, entity, MetricRowMeta(tm, metricKey))
}

case class MetricRowMeta(
  val tm: Long,
  override val metricKey: String,
  val entityUniqueCount: Long = 0,
  val entityCount: Long = 0,
  val metricCount: Double = 0.0)
    extends HasMetricKey
