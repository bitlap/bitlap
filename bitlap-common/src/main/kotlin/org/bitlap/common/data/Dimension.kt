/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.data

/**
 * Desc: Common dimension in [Event]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/8
 */
data class Dimension(private val pairs: Map<String, String> = emptyMap()) {

    val dimensions by lazy {
        mutableMapOf<String, String>()
            .apply { putAll(pairs) }
            .toSortedMap()
    }

    constructor(vararg pairs: Pair<String, String>) : this() {
        this.add(*pairs)
    }

    fun add(vararg pairs: Pair<String, String>): Dimension = this.also {
        pairs.forEach { dimensions[it.first] = it.second }
    }

    operator fun get(key: String) = this.dimensions[key]
    operator fun set(key: String, value: String): Dimension = this.also {
        this.dimensions[key] = value
    }

    fun firstPair(): Pair<String, String> {
        return this.dimensions.entries.first().toPair()
    }

    override fun toString(): String = dimensions.toString()

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as Dimension
        if (dimensions != other.dimensions) return false
        return true
    }

    override fun hashCode(): Int {
        return dimensions.hashCode()
    }
}
