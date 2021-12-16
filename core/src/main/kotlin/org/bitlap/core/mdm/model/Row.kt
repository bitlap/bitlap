package org.bitlap.core.mdm.model

import java.io.Serializable

/**
 * Desc: common row for fetcher
 */
class Row(val data: Array<*>) : Serializable {

    operator fun get(idx: Int): Any? {
        return this.data[idx]
    }

    fun getString(idx: Int): String? {
        return this[idx]?.toString()
    }

    override fun toString(): String {
        return data.contentToString()
    }
}
