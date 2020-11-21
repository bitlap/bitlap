package org.bitlap.common.bitmap

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
    private val _rbm: RoaringBitmap = RoaringBitmap()

    constructor()
    constructor(rbm: RoaringBitmap?) {
        if (rbm != null) {
            _rbm.or(rbm)
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

    override fun empty(): RBM = this.also { _rbm.clear() }
    override fun isEmpty(): Boolean = _rbm.isEmpty

    fun add(vararg dat: Int) = resetModify {
        this.also { _rbm.add(*dat) }
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

    override fun repair(): RBM = doIf(modified, this) {
        it.also {
            it._rbm.runOptimize()
            modified = false
        }
    }
    override fun getRBM(): RBM = this.also { it.repair() }
    override fun getNativeRBM(): RoaringBitmap = getRBM()._rbm
    override fun getCardinality(): Long = _rbm.longCardinality
    override fun getCardinalityUnique(): Long = _rbm.longCardinality

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
}
