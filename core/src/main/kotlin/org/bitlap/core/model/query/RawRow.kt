package org.bitlap.core.model.query

import org.bitlap.common.bitmap.BM
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.io.DefaultBitlapReader
import java.io.Serializable

/**
 * Desc: raw data row for [DefaultBitlapReader]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/30
 */
class RawRow(val data: Array<*>, val columns: List<String>) : Serializable {

    fun get(column: String): Any {
        val idx = columns.indexOf(column)
        if (idx < 0) {
            throw BitlapException("$column not found, columns is ${columns.toList()}")
        }
        return data[idx]!!
    }

    fun getString(column: String): String {
        return get(column).toString()
    }

    fun getBM(column: String): BM {
        return get(column) as BM
    }

    override fun toString(): String {
        return data.contentToString()
    }
}
