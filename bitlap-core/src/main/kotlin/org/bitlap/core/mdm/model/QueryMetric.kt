/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.model

/**
 * Desc: metric for query
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/4/11
 */
data class QueryMetric(
    val name: String,
    val aggType: AggType = AggType.None,
    val numeric: Boolean = false,
)

enum class AggType {
    None, Distinct, Count, CountDistinct, Sum, AVG
}
