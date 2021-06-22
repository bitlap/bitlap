package org.bitlap.core.model

import java.util.SortedMap

/**
 * Desc: Simple row for [org.bitlap.core.sdk.writer.SimpleBitlapWriter]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
data class SimpleRow( // TODO: 抽象层事件流
    val time: Long,
    val entity: Map<String, Int>,
    val dimension: Map<String, String>,
    val metric: Map<String, Double>,
) {

    /**
     * Transform to simple single rows
     */
    fun toSingleRows(): List<SimpleRowSingle> {
        if (entity.isEmpty() || metric.isEmpty()) {
            return emptyList()
        }
        return entity.flatMap { (ek, e) ->
            metric.map { (mk, m) ->
                SimpleRowSingle(time, ek, e, dimension.toSortedMap(), mk, m)
            }
        }
    }
}

data class SimpleRowSingle(
    val time: Long,
    val entityKey: String,
    val entity: Int,
    val dimension: SortedMap<String, String>,
    val metricKey: String,
    val metric: Double,
    var bucket: Int = 0
)
