package org.bitlap.core.storage

import java.io.Closeable

/**
 * Desc: Base store for bitlap metadata.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/16
 */
interface BitlapStore<T> : Closeable {

    fun open()

    /**
     * Store [rows] with time [tm] to persistent filesystem or other stores
     */
    fun store(tm: Long, rows: List<T>)
}
