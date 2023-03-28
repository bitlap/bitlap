/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.plan

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BM
import org.bitlap.common.utils.BMUtils
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.FetchPlan
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.Keyword
import kotlin.streams.toList

/**
 * Merge metrics with different dimensions into a single row
 */
class MetricMergeDimFetchPlan(override val subPlans: List<FetchPlan>) : AbsFetchPlan() {

    override fun execute(context: FetchContext): RowIterator {
        if (this.subPlans.isEmpty()) {
            return RowIterator.empty()
        }
        if (this.subPlans.size == 1) {
            return this.subPlans.last().execute(context)
        }
        val rowsSet = this.subPlans.parallelStream().map { it.execute(context) }.toList()

        return rowsSet.reduce { rs1, rs2 -> merge0(rs1, rs2) }
    }

    private fun merge0(rs1: RowIterator, rs2: RowIterator): RowIterator {
        // check key types should be different
        val keyTypes1 = rs1.keyTypes.map { it.name }.filter { it != Keyword.TIME }
        val keyTypes2 = rs2.keyTypes.map { it.name }.filter { it != Keyword.TIME }
        PreConditions.checkExpression(
            keyTypes1.intersect(keyTypes2.toSet()).isEmpty(),
            msg = "Row iterators key types need to be different, one is $keyTypes1, the other is $keyTypes2"
        )
        // check value types should be the same
        val valueTypes1 = rs1.valueTypes.map { it.name }.sorted()
        val valueTypes2 = rs2.valueTypes.map { it.name }.sorted()
        PreConditions.checkExpression(
            valueTypes1 == valueTypes2,
            msg = "Row iterators value types need to be the same, one is $valueTypes1, the other is $valueTypes2"
        )

        val resultKeyTypes = (rs1.keyTypes + rs2.keyTypes.filter { it.name != Keyword.TIME }).mapIndexed { idx, dt ->
            DataTypes.resetIndex(dt, idx)
        }
        val resultValueTypes = rs1.valueTypes.mapIndexed { idx, dt ->
            DataTypes.resetIndex(dt, resultKeyTypes.size + idx)
        }

        // let rows1 as cartesian or not
        val results = LinkedHashMap<List<Any?>, Row>()
        var rows1 = rs1
        var rows2 = rs2
        if (rs2.valueTypes.any { it is DataTypeCBM }) {
            rows1 = rs2
            rows2 = rs1
        }

        // let rows2 as Join Build Table
        // TODO: consider sort merge join
        val buffer = rows2.rows.asSequence().toList().groupBy { it[0] }
        for (r1 in rows1) {
            val keys1 = r1.getByTypes(rows1.keyTypes)
            val tmValue = keys1[0]
            if (buffer.containsKey(tmValue)) {
                for (r2 in buffer[tmValue]!!) {
                    val keys2 = r2.getByTypes(rows2.keyTypes)
                    val rKeys = keys1 + keys2.drop(1)
                    if (results.containsKey(rKeys)) {
                        // should never get here
                        continue
                    }
                    val cells = resultValueTypes.map { dt ->
                        val cell1 = r1[rows1.getType(dt.name).idx]
                        val cell2 = r2[rows2.getType(dt.name).idx]
                        BMUtils.and(cell1 as BM, cell2 as BM)
                    }
                    results[rKeys] = Row((rKeys + cells).toTypedArray())
                }
            } else {
                val rKeys = keys1 + arrayOfNulls(rows2.keyTypes.size - 1)
                val cells = resultValueTypes.map { dt -> r1[rows1.getType(dt.name).idx] }
                results[rKeys] = Row((rKeys + cells).toTypedArray())
            }
        }
        return RowIterator(BitlapIterator.of(results.values), resultKeyTypes, resultValueTypes)
    }

    override fun explain(depth: Int): String {
        return "${" ".repeat(depth)}+- MetricMergeDimFetchPlan\n${this.subPlans.joinToString("\n") { it.explain(depth + 2) }}"
    }
}
