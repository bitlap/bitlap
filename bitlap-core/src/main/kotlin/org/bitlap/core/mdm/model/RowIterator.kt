/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.mdm.model

import org.bitlap.common.BitlapIterator
import org.bitlap.core.mdm.format.DataType

/**
 * Iterator for [Row] with row type.
 */
class RowIterator(
    internal val rows: BitlapIterator<Row>,
    internal val keyTypes: List<DataType>,
    internal val valueTypes: List<DataType>,
) : BitlapIterator<Row>() {

    private val dataTypes = keyTypes + valueTypes
    private val dtNameMap = dataTypes.groupBy { it.name }.mapValues { it.value.first() }

    override fun hasNext(): Boolean {
        return this.rows.hasNext()
    }

    override fun next(): Row {
        return this.rows.next()
    }

    /**
     * get types for input strings.
     */
    fun getTypes(names: List<String>): List<DataType> {
        return names.map { this.dtNameMap[it]!! }
    }

    fun getType(name: String): DataType {
        return this.dtNameMap[name]!!
    }

    /**
     * get array result
     */
    fun toRows(projections: List<String>): Iterable<Array<Any?>> {
        val pTypes = projections.map {
            this.dtNameMap[it]
                ?: throw IllegalArgumentException("Input projections $projections contain a column that is not in current dataTypes $dataTypes.")
        }
        val rs = this.rows.asSequence().map { row ->
            arrayOfNulls<Any>(pTypes.size).also {
                pTypes.mapIndexed { i, c ->
                    it[i] = row[c.idx]
                }
            }
        }
        return rs.asIterable()
    }

    companion object {
        fun empty() = RowIterator(BitlapIterator.empty(), emptyList(), emptyList())
    }
}
