package org.bitlap.storage.store

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
     * Store [t] to persistent filesystem or other stores
     */
    fun store(t: T): T
}
