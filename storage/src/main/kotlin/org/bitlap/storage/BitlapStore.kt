package org.bitlap.storage.store

/**
 * Desc: Base store for bitlap metadata.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/16
 */
interface BitlapStore<T> {

    /**
     * Store [t] to persistent filesystem or other stores
     */
    fun store(t: T): T

    /**
     * Check [T] if exists
     */
    fun exists(t: T): Boolean

    /**
     * Get [t]
     */
    fun get(t: T): T
}