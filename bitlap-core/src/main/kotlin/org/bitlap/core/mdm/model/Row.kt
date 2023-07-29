/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.model

import org.bitlap.core.mdm.format.DataType
import java.io.Serializable

/**
 * Desc: common row for fetcher
 */
class Row(val data: Array<Any?>) : Serializable {

    constructor(size: Int) : this(arrayOfNulls(size))

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

    fun getByIdxs(idxs: List<Int>): List<Any?> {
        return idxs.map { this.data[it] }
    }

    fun getByTypes(types: List<DataType>): List<Any?> {
        return types.map { this.data[it.idx] }
    }

    override fun toString(): String {
        return data.contentToString()
    }
}
