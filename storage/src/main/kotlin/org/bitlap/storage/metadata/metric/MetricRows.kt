package org.bitlap.storage.metadata.metric

import org.bitlap.storage.metadata.MetricRow

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/1/26
 */
data class MetricRows(val tm: Long, val metrics: List<MetricRow>)
