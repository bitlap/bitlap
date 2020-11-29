package org.bitlap.common.bitmap

import org.bitlap.common.Versions
import org.bitlap.common.bitmap.rbm.RoaringArray
import org.bitlap.common.bitmap.rbm.RoaringBitmap
import org.bitlap.common.doIf
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteBuffer

/**
 * Desc:
 *   Build from RoaringBitmap[commit: 30897952]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */
open class RBM : AbsBM {

    /**
     * native data structure: RoaringBitmap
     */
    private var _rbm: RoaringBitmap = RoaringBitmap()

    constructor()
    constructor(rbm: RoaringBitmap?, copy: Boolean = false) {
        if (rbm != null) {
            _rbm = doIf(copy, rbm) {
                it.clone()
            }
        }
    }
    constructor(bytes: ByteArray?) {
        this.setBytes(bytes)
    }
    constructor(dat: IntArray) {
        _rbm.add(*dat)
    }
    constructor(rangeStart: Long, rangeEnd: Long) {
        _rbm.add(rangeStart, rangeEnd)
    }

    override fun empty(): RBM = resetModify {
        this.also { _rbm.clear() }
    }
    override fun trim(): RBM = resetModify {
        this.also { _rbm.trim() }
    }
    override fun isEmpty(): Boolean = _rbm.isEmpty

    fun add(dat: Int): RBM = resetModify {
        this.also { _rbm.add(dat) }
    }

    fun add(vararg dat: Int): RBM = resetModify {
        this.also { _rbm.add(*dat) }
    }

    fun addN(dat: IntArray, offset: Int, n: Int): RBM = resetModify {
        this.also { _rbm.addN(dat, offset, n) }
    }

    fun add(rangeStart: Long, rangeEnd: Long): RBM = resetModify {
        this.also { _rbm.add(rangeStart, rangeEnd) }
    }

    fun remove(dat: Int): RBM = resetModify {
        this.also { _rbm.remove(dat) }
    }

    override fun repair(): RBM = doIf(modified, this) {
        it.also {
            it._rbm.runOptimize()
            modified = false
        }
    }
    override fun getRBM(): RBM = this.also { it.repair() }
    fun getNativeRBM(): RoaringBitmap = _rbm
    override fun getCountUnique(): Long = _rbm.longCardinality
    override fun getCount(): Long = _rbm.longCardinality
    override fun getSizeInBytes(): Long = _rbm.longSizeInBytes + 1 // boolean

    override fun split(splitSize: Int, copy: Boolean): Map<Int, RBM> {
        if (splitSize <= 1 || _rbm.isEmpty) {
            return mutableMapOf(0 to doIf(copy, this) { it.clone() })
        }
        val results = mutableMapOf<Int, RBM>()
        val array = _rbm.highLowContainer
        (0 until array.size()).forEach { i ->
            val key = array.keys[i]
            val value = doIf(copy, array.values[i]) { it.clone() }
            val idx = key.toShort() % splitSize
            if (results.containsKey(idx)) {
                results[idx]!!._rbm.append(key, value)
            } else {
                results[idx] = RBM(RoaringBitmap(RoaringArray().also { it.append(key, value) }))
            }
        }
        return results
    }

    override fun getBytes(buffer: ByteBuffer?): ByteArray {
        this.repair()
        val buf = buffer ?: ByteBuffer.allocate(Int.SIZE_BYTES + _rbm.serializedSizeInBytes())
        buf.putInt(Versions.RBM_VERSION_V1)
        _rbm.serialize(buf)
        return buf.array()
    }

    override fun setBytes(bytes: ByteArray?): RBM = resetModify {
        this.also {
            if (bytes == null || bytes.isEmpty()) {
                _rbm.clear()
            } else {
                DataInputStream(ByteArrayInputStream(bytes)).use { dis ->
                    assert(dis.readInt() == Versions.RBM_VERSION_V1)
                    _rbm.deserialize(dis)
                }
            }
        }
    }

    override fun contains(dat: Int): Boolean = _rbm.contains(dat)
    override fun clone(): RBM = RBM(_rbm.clone())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as RBM
        if (_rbm != other._rbm) return false
        return true
    }

    override fun hashCode(): Int = _rbm.hashCode()
    override fun toString(): String = _rbm.toString()

    /**
     * Base operate functions
     */
    internal fun _and(bm: RBM): RBM = resetModify {
        this.also { _rbm.and(bm._rbm) }
    }
    internal fun _andNot(bm: RBM): RBM = resetModify {
        this.also { _rbm.andNot(bm._rbm) }
    }
    internal fun _or(bm: RBM): RBM = resetModify {
        this.also { _rbm.or(bm._rbm) }
    }
    internal fun _xor(bm: RBM): RBM = resetModify {
        this.also { _rbm.xor(bm._rbm) }
    }
    internal fun _orNot(bm: RBM, rangeEnd: Long): RBM = resetModify {
        this.also { _rbm.orNot(bm._rbm, rangeEnd) }
    }

    companion object {

        @JvmStatic
        fun or(vararg rbms: RBM): RBM {
            return RBM(RoaringBitmap.or(rbms.map { it._rbm }.iterator()))
        }
    }
}
