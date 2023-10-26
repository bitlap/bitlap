/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common.bitmap

import org.bitlap.common.doIf
import org.bitlap.common.utils.BMUtils
import org.bitlap.common.utils.PreConditions
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.nio.ByteBuffer

/**
 * Bucket Bitmap
 */

class BBM : AbsBM {

    val container = hashMapOf<Int, RBM>()

    /**
     * get unique bitmap from [[container]]
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
    constructor(bytes: ByteArray?) {
        this.setBytes(bytes)
    }

    override fun clear(): BBM = this.empty()
    override fun empty(): BBM = resetModify {
        this.also {
            container.clear()
            _rbm.empty()
        }
    }
    override fun trim(): BBM = resetModify {
        this.also {
            container.values.forEach { it.trim() }
            _rbm.empty()
        }
    }
    override fun isEmpty(): Boolean = container.values.all { it.isEmpty() }

    fun add(bucket: Int, vararg dats: Int): BBM = resetModify {
        this.also {
            container.computeIfAbsent(bucket) { RBM() }
                .add(*dats)
        }
    }
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
            container.entries.removeIf { e -> e.value.isEmpty() }
            container.values.forEach { o -> o.repair() }
            _rbm = BMUtils.or(container.values)
            modified = false
        }
    }
    override fun getRBM(): RBM = doIf(modified, _rbm) {
        this.repair()
        _rbm
    }

    override fun getCountUnique(): Long = getRBM().getCountUnique()
    override fun getCount(): Double = getLongCount().toDouble()
    override fun getLongCount(): Long = container.values.fold(0) { cnt, r -> cnt + r.getLongCount() }
    override fun getSizeInBytes(): Long {
        /** see [[getBytes]] */
        return container.values.fold(Int.SIZE_BYTES.toLong()) { size, r ->
            size + r.getSizeInBytes() + 2 + // ref
                Int.SIZE_BYTES + // mapKey
                Int.SIZE_BYTES // bytes length
        }
    }

    override fun split(splitSize: Int, copy: Boolean): Map<Int, BBM> {
        if (splitSize <= 1) {
            return hashMapOf(0 to doIf(copy, this) { this.clone() })
        }
        val results = hashMapOf<Int, BBM>()
        container.forEach { (bucket, rbm) ->
            val rs = rbm.split(splitSize, copy)
            rs.forEach { (index, r) ->
                val bbm = results.computeIfAbsent(index) { BBM() }
                bbm.container.computeIfAbsent(bucket) { RBM() }.or(r)
            }
        }
        return results
    }

    override fun getBytes(): ByteArray = getBytes(null)
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
                    PreConditions.checkExpression(dis.readInt() == Versions.BBM_VERSION_V1, msg = "Broken BBM bytes.")
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
    fun contains(bucket: Int, dat: Int): Boolean = container.containsKey(bucket) && container[bucket]!!.contains(dat)
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

    override fun toString(): String {
        return "BBM(count=${this.getCount()}, uniqueCount=${this.getCountUnique()})"
    }

    /**
     * operators
     */
    override fun and(bm: BM): BBM = resetModify {
        when (bm) {
            is RBM -> {
                if (bm.isEmpty()) {
                    container.clear()
                } else {
                    container.values.forEach { it.and(bm) }
                    container.entries.removeIf { it.value.isEmpty() }
                }
            }
            is BBM -> {
                if (bm.isMagic()) {
                    this.and(bm.container[MAGIC_BUCKET]!!)
                } else {
                    if (bm.isEmpty()) {
                        container.clear()
                    } else {
                        val magic = bm.container[MAGIC_BUCKET]
                        container.forEach { (bucket, rbm) ->
                            if (bm.container.containsKey(bucket)) {
                                rbm.and(bm.container[bucket]!!)
                                magic?.also { rbm.and(it) }
                            } else {
                                rbm.empty()
                            }
                        }
                    }
                }
                container.entries.removeIf { it.value.isEmpty() }
            }
            else -> throw IllegalArgumentException()
        }
        this
    }

    override fun andNot(bm: BM): BBM = resetModify {
        when (bm) {
            is RBM -> {
                if (!bm.isEmpty()) {
                    container.values.forEach { it.andNot(bm) }
                }
            }
            is BBM -> {
                if (bm.isMagic()) {
                    this.andNot(bm.container[MAGIC_BUCKET]!!)
                } else {
                    if (!bm.isEmpty()) {
                        val magic = bm.container[MAGIC_BUCKET]
                        container.forEach { (bucket, rbm) ->
                            if (bm.container.containsKey(bucket)) {
                                rbm.andNot(bm.container[bucket]!!)
                                magic?.also { rbm.andNot(it) }
                            }
                        }
                    }
                }
            }
            else -> throw IllegalArgumentException()
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }

    override fun or(bm: BM): BBM = resetModify {
        when (bm) {
            is RBM -> {
                if (container.isEmpty()) {
                    container[MAGIC_BUCKET] = bm.clone()
                } else if (!bm.isEmpty()) {
                    container.values.forEach { it.or(bm) }
                }
            }
            is BBM -> {
                if (bm.isMagic()) {
                    this.or(bm.container[MAGIC_BUCKET]!!)
                } else {
                    if (!bm.isEmpty()) {
                        val magic = bm.container[MAGIC_BUCKET]
                        bm.container.forEach { (bucket, rbm) ->
                            val tmp = container.computeIfAbsent(bucket) { RBM() }.or(rbm)
                            magic?.also { tmp.or(it) }
                        }
                    }
                }
            }
            else -> throw IllegalArgumentException()
        }
        this
    }

    override fun xor(bm: BM): BBM = resetModify {
        when (bm) {
            is RBM -> {
                if (container.isEmpty()) {
                    container[MAGIC_BUCKET] = bm.clone()
                } else if (!bm.isEmpty()) {
                    container.values.forEach { it.xor(bm) }
                }
            }
            is BBM -> {
                if (bm.isMagic()) {
                    this.xor(bm.container[MAGIC_BUCKET]!!)
                } else {
                    if (!bm.isEmpty()) {
                        val magic = bm.container[MAGIC_BUCKET]
                        bm.container.forEach { (bucket, rbm) ->
                            val tmp = container.computeIfAbsent(bucket) { RBM() }.xor(rbm)
                            magic?.also { tmp.xor(it) }
                        }
                    }
                }
            }
            else -> throw IllegalArgumentException()
        }
        container.entries.removeIf { it.value.isEmpty() }
        this
    }

    /**
     * Check BBM if only contains [[MAGIC_BUCKET]]
     */
    private fun isMagic(): Boolean {
        if (!container.containsKey(MAGIC_BUCKET)) {
            return false
        }
        container.entries.removeIf { it.value.isEmpty() }
        return container.size == 1 && container[MAGIC_BUCKET] != null
    }

    /**
     * operator functions
     */
    operator fun plusAssign(o: BM) {
        this.or(o)
    }
    operator fun plus(o: BM) = this.clone().or(o)
    operator fun minusAssign(o: BM) {
        this.andNot(o)
    }
    operator fun minus(o: BM) = this.clone().andNot(o)

    companion object {

        /**
         * magic bucket, -1 will be considered as [RBM]
         */
        const val MAGIC_BUCKET = -1

        @JvmStatic
        fun and(bm1: BBM, bm2: RBM): BBM = bm1.clone().and(bm2)
        @JvmStatic
        fun and(bm1: BBM, bm2: BBM): BBM = bm1.clone().and(bm2)
        @JvmStatic
        fun and(vararg bms: BBM): BBM {
            if (bms.isEmpty()) return BBM()
            val answer = bms.first().clone()
            bms.drop(1).forEach(answer::and)
            return answer
        }

        @JvmStatic
        fun andNot(bm1: BBM, bm2: RBM): BBM = bm1.clone().andNot(bm2)
        @JvmStatic
        fun andNot(bm1: BBM, bm2: BBM): BBM = bm1.clone().andNot(bm2)

        @JvmStatic
        fun or(bm1: BBM, bm2: RBM): BBM = bm1.clone().or(bm2)
        @JvmStatic
        fun or(bm1: BBM, bm2: BBM): BBM =
            when {
                bm1.isEmpty() -> bm2.clone()
                bm2.isEmpty() -> bm1.clone()
                else -> bm1.clone().or(bm2)
            }
        @JvmStatic
        fun or(vararg bms: BBM): BBM {
            if (bms.isEmpty()) return BBM()
            val answer = bms.first().clone()
            bms.drop(1).forEach(answer::or)
            return answer
        }

        @JvmStatic
        fun xor(bm1: BBM, bm2: RBM): BBM = bm1.clone().xor(bm2)
        @JvmStatic
        fun xor(bm1: BBM, bm2: BBM): BBM = bm1.clone().xor(bm2)
    }
}
