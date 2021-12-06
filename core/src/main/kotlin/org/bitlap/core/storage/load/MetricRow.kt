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

data class MetricRowMetaSimple(
    var entityUniqueCount: Long = 0,
    var entityCount: Long = 0,
    var metricCount: Double = 0.0
) {
    fun addEntityUniqueCount(entityUniqueCount: Long) = this.also {
        it.entityUniqueCount = it.entityUniqueCount + entityUniqueCount
    }

    fun addEntityCount(entityCount: Long) = this.also {
        it.entityCount = it.entityCount + entityCount
    }

    fun addMetricCount(metricCount: Double) = this.also {
        it.metricCount = it.metricCount + metricCount
    }
}
