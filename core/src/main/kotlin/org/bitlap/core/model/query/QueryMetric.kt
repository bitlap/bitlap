package org.bitlap.core.model.query

/**
 * Desc: metric for query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/11
 */
data class QueryMetric(
    val metricKey: String,
    val aggType: AggType = AggType.None
)

enum class AggType {
    None, Distinct, Count, CountDistinct, Sum, AVG
}
