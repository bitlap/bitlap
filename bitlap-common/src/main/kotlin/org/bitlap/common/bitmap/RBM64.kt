/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.bitmap

import org.bitlap.common.doIf
import org.bitlap.common.utils.PreConditions
import org.bitlap.roaringbitmap.longlong.LongConsumer
import org.bitlap.roaringbitmap.longlong.Roaring64Bitmap
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.nio.ByteBuffer

/**
 * 64 bit for RBM
 */
open class RBM64 : AbsBM {

    /**
     * native data structure: RoaringBitmap
     */
    private var _rbm = Roaring64Bitmap()

    constructor()
    constructor(rbm: Roaring64Bitmap?, copy: Boolean = false) {
        if (rbm != null) {
            _rbm = doIf(copy, rbm) {
                it.clone()
            }
        }
    }
    constructor(bytes: ByteArray?) {
        this.setBytes(bytes)
    }
    constructor(dat: LongArray) {
        _rbm.add(*dat)
    }
    constructor(rangeStart: Long, rangeEnd: Long) {
        _rbm.add(rangeStart, rangeEnd)
    }

    override fun clear(): RBM64 = this.empty()
    override fun empty(): RBM64 = resetModify {
        this.also { _rbm.clear() }
    }
    override fun trim(): RBM64 = resetModify {
        this.also { _rbm.trim() }
    }
    override fun isEmpty(): Boolean = _rbm.isEmpty

    fun add(dat: Long): RBM64 = resetModify {
        this.also { _rbm.add(dat) }
    }

    fun add(vararg dat: Long): RBM64 = resetModify {
        this.also { _rbm.add(*dat) }
    }

    fun add(rangeStart: Long, rangeEnd: Long): RBM64 = resetModify {
        this.also { _rbm.add(rangeStart, rangeEnd) }
    }

    fun remove(dat: Long): RBM64 = resetModify {
        this.also { _rbm.removeLong(dat) }
    }

    override fun repair(): RBM64 = doIf(modified, this) {
        it.also {
            it._rbm.runOptimize()
            modified = false
        }
    }
    override fun getRBM(): RBM = TODO("Not yet implemented")
    fun getNativeRBM(): Roaring64Bitmap = _rbm
    override fun getCountUnique(): Long = _rbm.longCardinality
    override fun getCount(): Double = _rbm.longCardinality.toDouble()
    override fun getLongCount(): Long = _rbm.longCardinality
    override fun getSizeInBytes(): Long = _rbm.longSizeInBytes + 1 // boolean

    override fun split(splitSize: Int, copy: Boolean): Map<Int, RBM64> {
        if (splitSize <= 1 || _rbm.isEmpty) {
            return hashMapOf(0 to doIf(copy, this) { it.clone() })
        }
        val results = hashMapOf<Int, RBM64>()
//        val array = _rbm.highLowContainer.highKeyIterator()
//        (0 until array.size()).forEach { i ->
//            val key = array.keys[i]
//            val value = doIf(copy, array.values[i]) { it.clone() }
//            val idx = key.toShort() % splitSize
//            if (results.containsKey(idx)) {
//                results[idx]!!._rbm.append(key, value)
//            } else {
//                results[idx] = RBM64(Roaring64Bitmap(RoaringArray().also { it.append(key, value) }))
//            }
//        }
        return results
    }

    override fun getBytes(): ByteArray = getBytes(null)
    override fun getBytes(buffer: ByteBuffer?): ByteArray {
        this.repair()
        val buf = buffer ?: ByteBuffer.allocate(Int.SIZE_BYTES + _rbm.serializedSizeInBytes().toInt()) // TODO (optimize)
        buf.putInt(Versions.RBM64_VERSION_V1)
        _rbm.serialize(buf)
        return buf.array()
    }

    override fun setBytes(bytes: ByteArray?): RBM64 = resetModify {
        this.also {
            if (bytes == null || bytes.isEmpty()) {
                _rbm.clear()
            } else {
                DataInputStream(ByteArrayInputStream(bytes)).use { dis ->
                    PreConditions.checkExpression(dis.readInt() == Versions.RBM64_VERSION_V1, msg = "Broken RBM64 bytes.")
                    _rbm.deserialize(dis)
                }
            }
        }
    }

    override fun contains(dat: Int): Boolean = _rbm.contains(dat.toLong())
    fun contains(dat: Long): Boolean = _rbm.contains(dat)
    override fun clone(): RBM64 = RBM64(_rbm.clone())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as RBM64
        if (_rbm != other._rbm) return false
        return true
    }

    override fun hashCode(): Int = _rbm.hashCode()
    override fun toString(): String = _rbm.toString()

    override fun and(bm: BM): RBM64 = resetModify {
        this.also { _rbm.and((bm as RBM64)._rbm) }
    }

    override fun andNot(bm: BM): RBM64 = resetModify {
        this.also { _rbm.andNot((bm as RBM64)._rbm) }
    }

    override fun or(bm: BM): RBM64 = resetModify {
        this.also { _rbm.or((bm as RBM64)._rbm) }
    }

    override fun xor(bm: BM): RBM64 = resetModify {
        this.also { _rbm.xor((bm as RBM64)._rbm) }
    }

    /**
     * Consume functions
     */
    fun forEach(accept: (Long) -> Unit) = _rbm.forEach(LongConsumer { accept(it) })
    fun toList(): MutableList<Long> {
        return mutableListOf<Long>().also { ref ->
            _rbm.forEach(LongConsumer { ref.add(it) })
        }
    }
    fun iterator() = Iterable { _rbm.iterator() }.iterator()

    fun iteratorReverse(): Iterator<Long> {
        val i = _rbm.reverseLongIterator
        return Iterable {
            object : Iterator<Long> {
                override fun hasNext() = i.hasNext()
                override fun next(): Long = i.next()
            }
        }.iterator()
    }
}
