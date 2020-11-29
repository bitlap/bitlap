package org.bitlap.common.bitmap

import org.bitlap.common.Versions
import org.bitlap.common.doIf
import org.bitlap.common.utils.BMUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

/**
 * Desc: Bucket Bitmap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */

class BBM : AbsBM {

    private val container = mutableMapOf<Int, RBM>()

    /**
     * get unique bitmap from [container]
     */
    private var _rbm = RBM()

    constructor()
    constructor(rbms: Map<Int, RBM>, copy: Boolean = false) {
        if (copy) {
            rbms.forEach { (k, v) -> container[k] = v.clone() }
        } else {
            container.putAll(rbms)
        }
    }
    constructor(bytes: ByteArray) {
        this.setBytes(bytes)
    }

    override fun empty(): BBM = resetModify {
        this.also { container.clear() }
    }
    override fun trim(): BBM = resetModify {
        this.also { container.values.forEach { it.trim() } }
    }
    override fun isEmpty(): Boolean = container.values.all { it.isEmpty() }

    fun add(bucket: Int, dat: Int) = add(bucket to dat)
    fun add(vararg dats: Pair<Int, Int>): BBM = resetModify {
        this.also {
            dats.forEach { (bucket, dat) ->
                container.computeIfAbsent(bucket) { RBM() }
                    .add(dat)
            }
        }
    }
    fun adds(vararg dats: Pair<Int, IntArray>): BBM = resetModify {
        this.also {
            dats.forEach { (bucket, dat) ->
                container.computeIfAbsent(bucket) { RBM() }
                    .add(*dat)
            }
        }
    }

    fun remove(bucket: Int, dat: Int): BBM = resetModify {
        this.also { container[bucket]?.remove(dat) }
    }
    fun remove(dat: Int): BBM = resetModify {
        this.also { container.values.forEach { it.remove(dat) } }
    }


    override fun repair(): BBM = doIf(modified, this) {
        it.also {
            container.entries.removeIf { it.value.isEmpty() }
            container.values.forEach { o -> o.repair() }
            modified = false
        }
    }
    override fun getRBM(): RBM = doIf(modified, _rbm) {
        repair()
        _rbm = BMUtils.or(container.values)
        _rbm
    }

    override fun getCountUnique(): Long = getRBM().getCountUnique()
    override fun getCount(): Long = container.values.fold(0) { cnt, r -> cnt + r.getCount() }
    override fun getSizeInBytes(): Long {
        /** see [getBytes] */
        return container.values.fold(Int.SIZE_BYTES.toLong()) { size, r ->
            size + r.getSizeInBytes() + 2 + // ref
                    Int.SIZE_BYTES +        // mapKey
                    Int.SIZE_BYTES          // bytes length
        }
    }

    override fun split(splitSize: Int, copy: Boolean): Map<Int, BBM> {
        if (splitSize <= 1) {
            return mutableMapOf(0 to doIf(copy, this) { this.clone() })
        }
        val results = mutableMapOf<Int, BBM>()
        container.forEach { (bit, rbm) ->
            val rs = rbm.split(splitSize, copy)
            rs.forEach { (index, r) ->
                val bbm = results.computeIfAbsent(index) { BBM() }
                bbm.container.computeIfAbsent(bit) { RBM() }.or(r)
            }
        }
        return results
    }

    override fun getBytes(buffer: ByteBuffer?): ByteArray {
        this.repair()
        val bos = ByteArrayOutputStream()
        DataOutputStream(bos).use { dos ->
            dos.writeInt(Versions.BBM_VERSION_V1)
            container.forEach { (b, r) ->
                dos.writeInt(b)
                val bytes = r.getBytes()
                dos.writeInt(bytes.size)
                dos.write(bytes)
            }
        }
        return bos.toByteArray()
    }

    override fun setBytes(bytes: ByteArray?): BBM = resetModify {
        this.also {
            if (bytes == null || bytes.isEmpty()) {
                container.clear()
            } else {
                DataInputStream(ByteArrayInputStream(bytes)).use { dis ->
                    assert(dis.readInt() == Versions.BBM_VERSION_V1)
                    while (dis.available() > 0) {
                        val bit = dis.readInt()
                        val rBytes = ByteArray(dis.readInt())
                        dis.read(rBytes)
                        container[bit] = RBM(rBytes)
                    }
                }
            }
        }
    }

    override fun contains(dat: Int): Boolean = container.values.any { it.contains(dat) }
    override fun clone(): BBM = BBM(container, copy = true)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as BBM
        if (container != other.container) return false
        if (_rbm != other._rbm) return false
        return true
    }

    override fun hashCode(): Int {
        var result = container.hashCode()
        result = 31 * result + _rbm.hashCode()
        return result
    }

    override fun toString(): String = container.toString()

    /**
     * Base operate functions
     */
    internal fun _and(bbm: BBM): BBM = resetModify {
        if (bbm.isEmpty()) {
            container.clear()
        } else {
            container.forEach { (bit, rbm) ->
                if (bbm.container.containsKey(bit)) {
                    rbm.and(bbm.container[bit]!!)
                } else {
                    rbm.empty()
                }
            }
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }
    internal fun _and(rbm: RBM): BBM = resetModify {
        if (rbm.isEmpty()) {
            container.clear()
        } else {
            container.values.forEach { it.and(rbm) }
            container.entries.removeIf { it.value.isEmpty() }
        }
        this
    }
    internal fun _andNot(bbm: BBM): BBM = resetModify {
        if (!bbm.isEmpty()) {
            container.forEach { (bit, rbm) ->
                if (bbm.container.containsKey(bit)) {
                    rbm._andNot(bbm.container[bit]!!)
                }
            }
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }
    internal fun _andNot(rbm: RBM): BBM = resetModify {
        if (!rbm.isEmpty()) {
            container.values.forEach { it._andNot(rbm) }
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }
    internal fun _or(bbm: BBM): BBM = resetModify {
        if (!bbm.isEmpty()) {
            bbm.container.forEach { (bit, rbm) ->
                container.computeIfAbsent(bit) { RBM() }.or(rbm)
            }
        }
        this
    }
    internal fun _or(rbm: RBM): BBM = resetModify {
        if (!rbm.isEmpty()) {
            container.values.forEach { it.or(rbm) }
        }
        this
    }
    internal fun _xor(bbm: BBM): BBM = resetModify {
        if (!bbm.isEmpty()) {
            bbm.container.forEach { (bit, rbm) ->
                container.computeIfAbsent(bit) { RBM() }._xor(rbm)
            }
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }
    internal fun _xor(rbm: RBM): BBM = resetModify {
        if (!rbm.isEmpty()) {
            container.values.forEach { it._xor(rbm) }
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }
}