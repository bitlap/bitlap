/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage.load

import org.apache.carbondata.sdk.file.CarbonReader
import org.bitlap.common.BitlapBatchIterator
import org.bitlap.common.utils.PreConditions

/**
 * Desc: Batch iterator for metric row
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/14
 */
open class MetricRowIterator<R>(private val reader: CarbonReader<Any>, private val rowHandler: (Array<*>) -> R?) : BitlapBatchIterator<R>() {

    @Volatile
    private var close = false

    override fun nextBatch(): List<R> {
        this.checkOpen()
        return this.reader.readNextBatchRow().mapNotNull {
            this.rowHandler.invoke(it as Array<*>)
        }
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
        try {
            this.reader.close()
        } catch (e: Exception) {
            // ignore
        }
    }

    private fun checkOpen() {
        PreConditions.checkExpression(!close, msg = "MetricRowIterator has been closed.")
    }
}
