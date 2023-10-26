/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.common.data

/**
 * Common dimension in [[Event]]
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
