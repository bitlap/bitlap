/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.storage.load.HasMetricKey

/**
 * metric or dimension container to manager kv pairs
 */
class MDContainer<I : HasMetricKey, R>(private val keySize: Int) : LinkedHashMap<List<Any?>, MutableMap<String, R>>() {

    fun put(key: Any, row: I, initial: (I) -> R, reducer: (R, I) -> R): MDContainer<I, R> {
        return this.put(listOf(key), row, initial, reducer)
    }

    fun put(key: List<Any?>, row: I, initial: (I) -> R, reducer: (R, I) -> R): MDContainer<I, R> {
        PreConditions.checkExpression(key.size == keySize)
        val value = computeIfAbsent(key) { mutableMapOf() }
        value[row.metricKey] =
            if (value.containsKey(row.metricKey)) {
                reducer(value[row.metricKey]!!, row)
            } else {
                initial(row)
            }
        return this
    }

    fun flatRows(metrics: List<String>, defaultValue: () -> Any): List<Row> {
        return this.map { (keys, value) ->
            arrayOfNulls<Any>(keySize + metrics.size).let {
                keys.forEachIndexed { i, key -> it[i] = key }
                metrics.mapIndexed { i, p ->
                    it[i + keySize] = value[p] ?: defaultValue()
                }
                Row(it)
            }
        }
    }
}
