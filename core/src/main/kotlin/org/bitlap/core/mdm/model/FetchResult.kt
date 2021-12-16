package org.bitlap.core.mdm.model

import org.bitlap.common.BitlapIterator

/**
 * Wrapper result for [org.bitlap.core.mdm.Fetcher]
 */
data class FetchResult(val rows: BitlapIterator<Row>, val dataType: List<String>) {

    /**
     * transform data to input columns
     */
    fun transform(columns: List<String>): FetchResult {
        val r = this.rows.asSequence().toList().map { row ->
            arrayOfNulls<Any>(columns.size).let {
                columns.mapIndexed { i, c ->
                    val idx = this.dataType.indexOf(c)
                    if (idx == -1) {
                        throw IllegalArgumentException("Input columns $columns contain a column that is not in current dataType $dataType.")
                    }
                    it[i] = row[idx] ?: RowValueMeta.empty()
                }
                Row(it)
            }
        }
        return FetchResult(BitlapIterator.of(r), columns)
    }
}
