/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common

import java.io.Closeable

/**
 * Desc: Common iterator
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/14
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
