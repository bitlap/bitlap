package org.bitlap.storage.metadata

/**
 * Desc: Metadata for [org.bitlap.storage.metadata.MetricRow]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/2/22
 */
data class MetricRowMeta(
    val tm: Long,
    val metricKey: String,
    val entityKey: String,
    val entityUniqueCount: Long = 0,
    val entityCount: Long = 0,
    val metricCount: Double = 0.0
)
