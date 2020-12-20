package org.bitlap.core

import java.io.Closeable

/**
 * Desc: Bitlap writer to handle data
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
interface BitlapWriter<T> : Closeable {

    fun write(t: T)
    fun write(ts: List<T>)

    override fun close()
}
