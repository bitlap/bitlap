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
package org.bitlap.common

import java.io.Closeable

/**
 * Common iterator
 */
abstract class BitlapIterator<E> : Iterator<E>, Closeable {

    protected fun initialize() {
        // ignore
    }

    override fun close() {
        // ignore
    }

    companion object {
        @JvmStatic
        fun <R> empty() = object : BitlapIterator<R>() {
            override fun next(): R = null!! // should never be here
            override fun hasNext(): Boolean = false
        }

        @JvmStatic
        fun <R> of(rows: Iterable<R>) = object : BitlapIterator<R>() {
            private val it = rows.iterator()
            override fun next(): R = it.next()
            override fun hasNext(): Boolean = it.hasNext()
        }

        @JvmStatic
        fun <R> batch(rows: Iterable<R>, batchSize: Int = 100) = object : BitlapBatchIterator<R>() {
            private val splits = rows.chunked(batchSize).iterator()
            override fun hasNext() = splits.hasNext() || super.hasNext()
            override fun nextBatch(): List<R> = splits.next()
        }
    }
}

abstract class BitlapBatchIterator<E> : BitlapIterator<E>() {

    private var count = 0
    private var index = 0
    private var rows: List<E>? = null

    protected abstract fun nextBatch(): List<E>

    override fun hasNext(): Boolean {
        return rows != null && index < rows!!.size
    }

    override fun next(): E {
        if (rows == null || index >= rows!!.size) {
            rows = this.nextBatch()
            index = 0
        }
        count++
        return rows!![index++]
    }
}
