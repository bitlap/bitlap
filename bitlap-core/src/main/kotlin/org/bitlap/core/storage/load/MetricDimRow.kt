/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage.load

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM

/**
 * Desc: One row for metric data model with one dimension.
 */
data class MetricDimRow(
    val tm: Long,
    override val metricKey: String,
    val dimensionKey: String,
    val dimension: String,
    val metric: CBM,
    val entity: BBM,
    var metadata: MetricDimRowMeta = MetricDimRowMeta(tm, metricKey, dimensionKey, dimension),
) : HasMetricKey

data class MetricDimRowMeta(
    val tm: Long,
    override val metricKey: String,
    val dimensionKey: String,
    val dimension: String,
    val entityUniqueCount: Long = 0,
    val entityCount: Long = 0,
    val metricCount: Double = 0.0
) : HasMetricKey
