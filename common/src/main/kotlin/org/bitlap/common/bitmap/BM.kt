package org.bitlap.common.bitmap

import org.bitlap.common.bitmap.rbm.RoaringBitmap
import java.io.Externalizable
import java.io.Serializable
import java.nio.ByteBuffer

/**
 * Desc: BM
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */
interface BM<T> : Serializable, Externalizable {

    fun empty(): T
    fun trim(): T
    fun isEmpty(): Boolean

    fun and(bm: BM<T>): T
    fun andNot(bm: BM<T>): T
    fun or(bm: BM<T>): T
    fun xor(bm: BM<T>): T
    fun orNot(bm: BM<T>, rangeEnd: Long): T

    fun repair(): T
    fun getCount(): Long
    fun getCountUnique(): Long
    fun getNativeRBM(): RoaringBitmap
    fun getSizeInBytes(): Long
    fun split(splitSize: Int, copy: Boolean = false): Map<Int, T>

    fun getBytes(buffer: ByteBuffer? = null): ByteArray
    fun setBytes(bytes: ByteArray? = null): T

    fun contains(i: Int): Boolean
    fun clone(): T
}
