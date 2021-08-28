package org.bitlap.common.bitmap

import org.bitlap.common.bitmap.rbm.FastAggregation
import org.bitlap.common.bitmap.rbm.IntConsumer
import org.bitlap.common.bitmap.rbm.RoaringArray
import org.bitlap.common.bitmap.rbm.RoaringBitmap
import org.bitlap.common.doIf
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteBuffer
import kotlin.math.max

/**
 * Desc:
 *   Build from RoaringBitmap[commit: fc3754f2]
 *
 * RBM number: support: 0, 1, ..., 2147483647, -2147483648, -2147483647,..., -1
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

    override fun clear(): RBM = this.empty()
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
    override fun getCount(): Double = _rbm.longCardinality.toDouble()
    override fun getLongCount(): Long = _rbm.longCardinality
    override fun getSizeInBytes(): Long = _rbm.longSizeInBytes + 1 // boolean

    override fun split(splitSize: Int, copy: Boolean): Map<Int, RBM> {
        if (splitSize <= 1 || _rbm.isEmpty) {
            return hashMapOf(0 to doIf(copy, this) { it.clone() })
        }
        val results = hashMapOf<Int, RBM>()
        val array = _rbm.highLowContainer
        (0 until array.size()).forEach { i ->
            val key = array.keys[i]
            val value = doIf(copy, array.values[i]) { it.clone() }
            val idx = key.toInt() % splitSize
            if (results.containsKey(idx)) {
                results[idx]!!._rbm.append(key, value)
            } else {
                results[idx] = RBM(RoaringBitmap(RoaringArray().also { it.append(key, value) }))
            }
        }
        return results
    }

    override fun getBytes(): ByteArray = getBytes(null)
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

    override operator fun contains(dat: Int): Boolean = _rbm.contains(dat)
    override fun clone(): RBM = RBM(_rbm.clone())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as RBM
        if (_rbm != other._rbm) return false
        return true
    }

    override fun hashCode(): Int = _rbm.hashCode()
    override fun toString(): String {
        return "RBM(count=${this.getCount()}, uniqueCount=${this.getCountUnique()})"
    }

    override fun and(bm: BM): RBM = resetModify {
        this.also { _rbm.and(bm.getRBM()._rbm) }
    }

    override fun andNot(bm: BM): RBM = resetModify {
        this.also { _rbm.andNot(bm.getRBM()._rbm) }
    }

    override fun or(bm: BM): RBM = resetModify {
        this.also { _rbm.or(bm.getRBM()._rbm) }
    }

    override fun xor(bm: BM): RBM = resetModify {
        this.also { _rbm.xor(bm.getRBM()._rbm) }
    }

    fun orNot(bm: RBM, rangeEnd: Long): RBM = resetModify {
        this.also { _rbm.orNot(bm._rbm, rangeEnd) }
    }

    /**
     * Consume functions
     */
    fun forEach(accept: (Int) -> Unit) = _rbm.forEach(IntConsumer { accept(it) })
    fun toList(): MutableList<Int> {
        return mutableListOf<Int>().also { ref ->
            _rbm.forEach(IntConsumer { ref.add(it) })
        }
    }
    fun iterator() = Iterable { _rbm.iterator() }.iterator()

    /** You can also use [iterator].chunk([bacthSize]) */
    fun <T> iteratorBatch(batchSize: Int, transform: (List<Int>) -> T): List<T> {
        val result = mutableListOf<T>()
        val iterator = _rbm.batchIterator
        val buffer = IntArray(max(batchSize, 1))
        // batch iterator
        val batch = ArrayList<Int>(batchSize)
        while (iterator.hasNext()) {
            val size = iterator.nextBatch(buffer)
            if (size > 0) {
                (0 until size).forEach { batch.add(buffer[it]) }
                result.add(transform.invoke(batch))
                batch.clear()
            }
        }
        return result
    }
    fun iteratorReverse(): Iterator<Int> {
        val i = _rbm.reverseIntIterator
        return Iterable {
            object : Iterator<Int> {
                override fun hasNext() = i.hasNext()
                override fun next(): Int = i.next()
            }
        }.iterator()
    }

    /**
     * operator functions
     */
    operator fun plusAssign(o: BM) {
        this.or(o)
    }
    operator fun plusAssign(dat: Int) {
        this.add(dat)
    }
    operator fun plus(o: BM) = this.clone().or(o)
    operator fun plus(dat: Int) = this.clone().add(dat)

    operator fun minusAssign(o: BM) {
        this.andNot(o)
    }
    operator fun minusAssign(dat: Int) {
        this.remove(dat)
    }
    operator fun minus(o: BM) = this.clone().andNot(o)
    operator fun minus(dat: Int) = this.clone().remove(dat)

    companion object {
        @JvmStatic
        fun and(bm1: RBM, bm2: RBM): RBM = RBM(RoaringBitmap.and(bm1._rbm, bm2._rbm))
        @JvmStatic
        fun and(vararg bms: RBM): RBM = RBM(FastAggregation.and(bms.map { it._rbm }.iterator()))
        @JvmStatic
        fun andCount(bm1: RBM, bm2: RBM): Long = RoaringBitmap.andCardinality(bm1._rbm, bm2._rbm).toLong()

        @JvmStatic
        fun andNot(bm1: RBM, bm2: RBM): RBM = RBM(RoaringBitmap.andNot(bm1._rbm, bm2._rbm))
        @JvmStatic
        fun andNotCount(bm1: RBM, bm2: RBM): Long = RoaringBitmap.andNotCardinality(bm1._rbm, bm2._rbm).toLong()

        @JvmStatic
        fun or(bm1: RBM, bm2: RBM): RBM = RBM(RoaringBitmap.or(bm1._rbm, bm2._rbm))
        @JvmStatic
        fun or(vararg bms: RBM): RBM = RBM(RoaringBitmap.or(bms.map { it._rbm }.iterator()))
        @JvmStatic
        fun orCount(bm1: RBM, bm2: RBM): Long = RoaringBitmap.orCardinality(bm1._rbm, bm2._rbm).toLong()

        @JvmStatic
        fun xor(bm1: RBM, bm2: RBM): RBM = RBM(RoaringBitmap.xor(bm1._rbm, bm2._rbm))
        @JvmStatic
        fun xor(vararg bms: RBM): RBM = RBM(FastAggregation.xor(bms.map { it._rbm }.iterator()))
        @JvmStatic
        fun xorCount(bm1: RBM, bm2: RBM): Long = RoaringBitmap.xorCardinality(bm1._rbm, bm2._rbm).toLong()
    }
}
