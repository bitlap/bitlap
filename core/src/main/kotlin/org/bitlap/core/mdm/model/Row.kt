package org.bitlap.core.mdm.model

import org.bitlap.core.mdm.format.DataType
import java.io.Serializable

/**
 * Desc: common row for fetcher
 */
class Row(val data: Array<Any?>) : Serializable {

    operator fun set(idx: Int, value: Any?) {
        this.data[idx] = value
    }

    operator fun get(idx: Int): Any? {
        return this.data[idx]
    }

    operator fun get(type: DataType): Any? {
        return this.data[type.idx]
    }

    fun getString(idx: Int): String? {
        return this[idx]?.toString()
    }

    override fun toString(): String {
        return data.contentToString()
    }
}
