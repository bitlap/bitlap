package org.bitlap.core.storage.load

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM

/**
 * Desc: One row for metric data model.
 */
data class MetricRow(
    val tm: Long,
    val metricKey: String,
    val metric: CBM,
    val entity: BBM,
    var metadata: MetricRowMeta,
)

data class MetricRowMeta(
    val tm: Long,
    val metricKey: String,
    val entityUniqueCount: Long = 0,
    val entityCount: Long = 0,
    val metricCount: Double = 0.0
)
