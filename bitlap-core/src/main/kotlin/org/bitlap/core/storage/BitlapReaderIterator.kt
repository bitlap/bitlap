/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import org.bitlap.common.BitlapBatchIterator
import org.bitlap.common.utils.PreConditions

/**
 * Desc: Batch iterator for metric row
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/14
 */
open class BitlapReaderIterator<R>(private val reader: BitlapReader<R>, private val limit: Int) : BitlapBatchIterator<R>() {

    @Volatile
    private var close = false

    override fun nextBatch(): List<R> {
        this.checkOpen()
        return this.reader.read(limit)
    }

    override fun hasNext(): Boolean {
        this.checkOpen()
        val has = this.reader.hasNext() || super.hasNext()
        // auto close
        if (!has) {
            this.close()
        }
        return has
    }

    override fun next(): R {
        this.checkOpen()
        return super.next()
    }

    override fun close() {
        if (close) {
            return
        }
        close = true
        this.reader.use { }
    }

    private fun checkOpen() {
        PreConditions.checkExpression(!close, msg = "BitlapReaderIterator has been closed.")
    }
}
