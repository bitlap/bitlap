/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.bitmap

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
interface BM : Serializable, Externalizable {

    fun clear(): BM
    fun empty(): BM
    fun trim(): BM
    fun isEmpty(): Boolean

    fun repair(): BM
    fun getCount(): Double
    fun getLongCount(): Long
    fun getCountUnique(): Long
    fun getRBM(): RBM
    fun getSizeInBytes(): Long
    fun split(splitSize: Int, copy: Boolean = false): Map<Int, BM>

    /**
     * serialize
     */
    fun getBytes(buffer: ByteBuffer?): ByteArray
    fun getBytes(): ByteArray
    fun setBytes(bytes: ByteArray? = null): BM

    /**
     * operators
     */
    fun and(bm: BM): BM
    fun andNot(bm: BM): BM
    fun or(bm: BM): BM
    fun xor(bm: BM): BM

    fun contains(dat: Int): Boolean
    fun clone(): BM
}
