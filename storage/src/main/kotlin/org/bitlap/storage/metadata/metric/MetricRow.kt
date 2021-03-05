package org.bitlap.storage.metadata

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.storage.metadata.metric.MetricRowMeta

/**
 * Desc: One row for metric data model.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/1/26
 */
data class MetricRow(
        val metricKey: String,
        val entityKey: String,
        val tm: Long,
        val metric: CBM,
        val entity: BBM,
        var metadata: MetricRowMeta,
)
