package org.bitlap.storage.load

import org.apache.carbondata.sdk.file.CarbonReader
import org.bitlap.common.BitlapBatchIterator

/**
 * Desc: Batch iterator for metric row
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/7/14
 */
open class MetricRowIterator<R>(private val reader: CarbonReader<Any>, private val rowHandler: (Array<*>) -> R) : BitlapBatchIterator<R>() {

    override fun nextBatch(): List<R> {
        return this.reader.readNextBatchRow().map {
            this.rowHandler.invoke(it as Array<*>)
        }
    }

    override fun hasNext(): Boolean {
        return this.reader.hasNext() || super.hasNext()
    }

    override fun close() {
        try {
            this.reader.close()
        } catch (e: Exception) {
            // ignore
        }
    }
}
