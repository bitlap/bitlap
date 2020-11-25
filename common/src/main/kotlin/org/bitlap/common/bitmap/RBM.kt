package org.bitlap.common.bitmap

import org.bitlap.common.bitmap.rbm.RoaringArray
import org.bitlap.common.bitmap.rbm.RoaringBitmap
import org.bitlap.common.doIf
import java.nio.ByteBuffer

/**
 * Desc:
 *   Build from RoaringBitmap[commit: 30897952]
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */
open class RBM : AbsBM<RBM> {

    /**
     * native data structure: RoaringBitmaps
     */
    private var _rbm: RoaringBitmap = RoaringBitmap()

    constructor()
    constructor(rbm: RoaringBitmap?, copy: Boolean = false) {
        if (rbm != null) {
            _rbm = doIf(copy, rbm) {
                _rbm.or(it)
                _rbm
            }
        }
    }
    constructor(bytes: ByteArray?) {
        this.setBytes(bytes)
    }
    constructor(dat: IntArray) {
        _rbm.add(*dat)
    }
    constructor(rangeStart: Int, rangeEnd: Int) {
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

    /**
     * Base operate functions
     */
    override fun and(bm: BM<RBM>): RBM = resetModify {
        this.also { _rbm.and(bm.getNativeRBM()) }
    }

    override fun andNot(bm: BM<RBM>): RBM = resetModify {
        this.also { _rbm.andNot(bm.getNativeRBM()) }
    }

    override fun or(bm: BM<RBM>): RBM = resetModify {
        this.also { _rbm.or(bm.getNativeRBM()) }
    }

    override fun xor(bm: BM<RBM>): RBM = resetModify {
        this.also { _rbm.xor(bm.getNativeRBM()) }
    }

    override fun orNot(bm: BM<RBM>, rangeEnd: Long): RBM = resetModify {
        this.also { _rbm.orNot(bm.getNativeRBM(), rangeEnd) }
    }

    override fun repair(): RBM = doIf(modified, this) {
        it.also {
            it._rbm.runOptimize()
            modified = false
        }
    }
    override fun getNativeRBM(): RoaringBitmap = _rbm
    override fun getCountUnique(): Long = _rbm.longCardinality
    override fun getCount(): Long = _rbm.longCardinality
    override fun getSizeInBytes(): Long = _rbm.longSizeInBytes

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
        val buf = buffer ?: ByteBuffer.allocate(_rbm.serializedSizeInBytes())
        _rbm.serialize(buf)
        return buf.array()
    }

    override fun setBytes(bytes: ByteArray?): RBM = resetModify {
        this.also {
            if (bytes == null || bytes.isEmpty()) {
                _rbm.clear()
            } else {
                _rbm.deserialize(ByteBuffer.wrap(bytes))
            }
        }
    }

    override fun contains(i: Int): Boolean = _rbm.contains(i)
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
}
