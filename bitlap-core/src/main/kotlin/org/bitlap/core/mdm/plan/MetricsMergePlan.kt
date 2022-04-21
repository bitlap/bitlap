/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BM
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.FetchPlan
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.mdm.model.RowValueMeta
import kotlin.streams.toList

/**
 * Merge metrics with same dimensions into a single row
 */
class MetricsMergePlan(override val subPlans: List<FetchPlan>) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        if (this.subPlans.isEmpty()) {
            return RowIterator.empty()
        }
        if (this.subPlans.size == 1) {
            return this.subPlans.first().execute(context)
        }
        val rowsSet = this.subPlans.parallelStream().map { it.execute(context) }.toList()
        val resultKeyTypes = this.getKeyTypes(rowsSet).mapIndexed { idx, dt ->
            DataTypes.resetIndex(dt, idx)
        }
        val resultValueTypes = this.getValueTypes(rowsSet).mapIndexed { idx, dt ->
            DataTypes.resetIndex(dt, resultKeyTypes.size + idx)
        }
        val results = LinkedHashMap<List<Any?>, Row>()
        var offset = 0
        for (rs in rowsSet) {
            val keyTypes = rs.fixTypes(resultKeyTypes) // fix key types
            val valueTypes = rs.valueTypes
            rs.rows.forEach { row ->
                val keys = keyTypes.map { row[it] }
                if (results.containsKey(keys)) {
                    val r = results[keys]!!
                    valueTypes.forEachIndexed { idx, vt ->
                        val i = idx + offset + keys.size
                        val cell = r[i]
                        // merge cells
                        if (cell == null) {
                            r[i] = row[vt.idx]
                        } else {
                            when (cell) {
                                is BM ->
                                    r[i] = cell.or(row[vt.idx] as BM)
                                is RowValueMeta ->
                                    r[i] = cell.add(row[vt.idx] as RowValueMeta)
                                else ->
                                    throw IllegalArgumentException("Illegal input types: ${cell::class.java}")
                            }
                        }
                    }
                } else {
                    results[keys] = arrayOfNulls<Any>(resultKeyTypes.size + resultValueTypes.size).let {
                        keys.forEachIndexed { idx, key ->
                            it[idx] = key
                        }
                        valueTypes.forEachIndexed { idx, vt ->
                            it[idx + offset + keys.size] = row[vt.idx]
                        }
                        Row(it)
                    }
                }
            }
            offset += valueTypes.size
        }
        return RowIterator(BitlapIterator.of(results.values), resultKeyTypes, resultValueTypes)
    }

    private fun getKeyTypes(rowsSet: List<RowIterator>): List<DataType> {
        PreConditions.checkNotEmpty(rowsSet)
        val keyNames = rowsSet
            .map { r -> r.keyTypes.map { it.name }.sorted() }
            .reduce { a, b ->
                PreConditions.checkExpression(
                    a == b,
                    msg = "Row iterators key types need to be the same, one is $a, the other is $b."
                )
                a
            }
        return rowsSet.first().getTypes(keyNames)
    }

    private fun getValueTypes(rowsSet: List<RowIterator>): List<DataType> {
        PreConditions.checkNotEmpty(rowsSet)
        return rowsSet
            .map { r -> r.valueTypes }
            .reduce { a, b -> a + b }
    }
}
