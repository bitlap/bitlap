package org.bitlap.core.model

/**
 * Desc: Simple row for [org.bitlap.core.sdk.writer.SimpleBitlapWriter]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
data class SimpleRow(
        val time: Long,
        val entity: Map<String, Int>,
        val dimension: Map<String, String>,
        val metric: Map<String, Double>
)
