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
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min

/**
 * Count Bitmap, only support positive count values.
 */

class CBM : AbsBM, ComparableBM {

    val container = hashMapOf<Int, BBM>()
    private var _bbm = BBM()
    var maxBit = 0
        private set
    var weight = 1.0

    constructor()
    constructor(weight: Double) {
        this.weight = weight
    }
    constructor(bbms: Map<Int, BBM>, copy: Boolean = false, weight: Double = 1.0) {
        if (copy) {
            bbms.forEach { (k, v) -> container[k] = v.clone() }
        } else {
            container.putAll(bbms)
        }
        this.weight = weight
        this.updateMaxBit()
    }
    constructor(bytes: ByteArray?) {
        this.setBytes(bytes)
    }

    override fun clear(): CBM = this.empty()
    override fun empty(): CBM = resetModify {
        this.also {
            container.clear()
            _bbm.empty()
            maxBit = 0
            weight = 1.0
        }
    }
    override fun trim(): CBM = resetModify {
        this.also {
            container.values.forEach { it.trim() }
            _bbm.empty()
            maxBit = 0
            weight = 1.0
        }
    }
    override fun isEmpty() = container.values.all { it.isEmpty() }
    override fun repair(): CBM = doIf(modified, this) {
        it.also {
            container.entries.removeIf { e -> e.value.isEmpty() }
            container.values.forEach { o -> o.repair() }
            _bbm = BMUtils.or(container.values)
            modified = false
        }
    }

    fun getBBM(): BBM = doIf(modified, _bbm) {
        this.repair()
        _bbm
    }
    override fun getRBM(): RBM = getBBM().getRBM()
    override fun getCount(): Double = weight * container.keys.map { container[it]!!.getLongCount() shl it }.sum()
    override fun getLongCount(): Long = getCount().toLong()
    override fun getCountUnique(): Long = getRBM().getCountUnique()
    override fun getSizeInBytes(): Long {
        /** see [getBytes] */
        return container.values.fold(Int.SIZE_BYTES.toLong() + Double.SIZE_BYTES) { size, r ->
            size + r.getSizeInBytes() + 2 + // ref
                Int.SIZE_BYTES + // mapKey
                Int.SIZE_BYTES // bytes length
        }
    }

    override fun split(splitSize: Int, copy: Boolean): Map<Int, CBM> {
        if (splitSize <= 1) {
            return hashMapOf(0 to doIf(copy, this) { this.clone() })
        }
        val results = hashMapOf<Int, CBM>()
        container.forEach { (bit, bbm) ->
            val bs = bbm.split(splitSize, copy)
            bs.forEach { (index, b) ->
                val cbm = results.computeIfAbsent(index) { CBM(this.weight) }
                cbm.container.computeIfAbsent(bit) { BBM() }.or(b)
            }
        }
        return results
    }

    /**
     * serialize
     */
    override fun getBytes(): ByteArray = getBytes(null)
    override fun getBytes(buffer: ByteBuffer?): ByteArray {
        this.repair()
        val bos = ByteArrayOutputStream()
        DataOutputStream(bos).use { dos ->
            dos.writeInt(Versions.CBM_VERSION_V1)
            dos.writeDouble(weight)
            container.forEach { (bit, bbm) ->
                dos.writeInt(bit)
                val bytes = bbm.getBytes()
                dos.writeInt(bytes.size)
                dos.write(bytes)
            }
        }
        return bos.toByteArray()
    }

    override fun setBytes(bytes: ByteArray?): CBM = resetModify {
        this.also {
            if (bytes == null || bytes.isEmpty()) {
                container.clear()
            } else {
                DataInputStream(ByteArrayInputStream(bytes)).use { dis ->
                    PreConditions.checkExpression(dis.readInt() == Versions.CBM_VERSION_V1, msg = "Broken CBM bytes.")
                    weight = dis.readDouble()
                    while (dis.available() > 0) {
                        val bit = dis.readInt()
                        val bbmBytes = ByteArray(dis.readInt())
                        dis.read(bbmBytes)
                        container[bit] = BBM(bbmBytes)
                    }
                }
            }
        }
    }

    fun updateMaxBit() {
        container.entries.removeIf { it.value.isEmpty() }
        maxBit = container.keys.maxOrNull() ?: 0
    }

    override fun contains(dat: Int): Boolean = container.values.any { it.contains(dat) }
    override fun clone() = CBM(container, true, weight)
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as CBM
        if (container != other.container) return false
        if (_bbm != other._bbm) return false
        if (maxBit != other.maxBit) return false
        if (weight != other.weight) return false

        return true
    }

    override fun hashCode(): Int {
        var result = container.hashCode()
        result = 31 * result + _bbm.hashCode()
        result = 31 * result + maxBit
        result = 31 * result + weight.hashCode()
        return result
    }

    override fun toString(): String {
        return "CBM(weight=$weight, maxBit=$maxBit, count=${this.getCount()}, uniqueCount=${this.getCountUnique()})"
    }

    /**
     * operators
     */
    fun add(dat: Int, count: Long): CBM = this.add(BBM.MAGIC_BUCKET, dat, count)
    fun add(bucket: Int, dat: Int, count: Long): CBM = resetModify {
        PreConditions.checkExpression(count > 0, "CBM count should be greater than 0")

        val bits = BMUtils.oneBitPositions(count)
        var carrier = false
        var currentIndex = 0
        var currentBit1 = bits[currentIndex]
        var currentBit2 = 0

        while (currentBit2 <= maxBit || currentIndex < bits.size || carrier) {
            val bbm = container.computeIfAbsent(currentBit2) { BBM() }
            if (currentBit1 == currentBit2) {
                if (!carrier) {
                    if (bbm.contains(bucket, dat)) {
                        bbm.remove(bucket, dat)
                        carrier = true
                    } else {
                        bbm.add(bucket, dat)
                    }
                }
                currentIndex += 1
                if (currentIndex < bits.size) {
                    currentBit1 = bits[currentIndex]
                }
            } else if (currentBit2 > currentBit1) {
                if (carrier) {
                    if (bbm.contains(bucket, dat)) {
                        bbm.remove(bucket, dat)
                    } else {
                        bbm.add(bucket, dat)
                        carrier = false
                    }
                } else {
                    break
                }
            } else if (carrier) {
                if (bbm.contains(bucket, dat)) {
                    bbm.remove(bucket, dat)
                } else {
                    bbm.add(bucket, dat)
                    carrier = false
                }
            }
            currentBit2 += 1
        }
        // maxBit = currentBit2 - 1
        maxBit = container.keys.maxOrNull() ?: 0
        this
    }

    override fun and(bm: BM): CBM = resetModify {
        when (bm) {
            is RBM, is BBM -> {
                container.values.forEach { it.and(bm) }
            }
            is CBM -> {
                TODO("Not yet implemented, andMin, andMax, andEquals")
            }
            else -> throw IllegalArgumentException()
        }
        updateMaxBit()
        this
    }

    override fun andNot(bm: BM): CBM = resetModify {
        when (bm) {
            is RBM, is BBM -> {
                container.values.forEach { it.andNot(bm) }
            }
            else -> throw IllegalArgumentException()
        }
        updateMaxBit()
        this
    }

    override fun or(bm: BM): CBM = resetModify {
        when (bm) {
            is RBM, is BBM -> {
//                if (!bm.isEmpty()) {
//                    container.values.forEach { it.or(bm) }
//                }
                throw IllegalArgumentException()
            }
            is CBM -> {
                if (this.isEmpty()) {
                    this.weight = bm.weight
                    bm.container.forEach { (k, v) -> container[k] = v.clone() }
                } else if (!bm.isEmpty()) {
                    if (this.hasWeight() || bm.hasWeight()) {
                        mergeWithWeight(bm)
                    } else {
                        mergeWithoutWeight(bm)
                    }
                }
            }
            else -> throw IllegalArgumentException()
        }
        updateMaxBit()
        this
    }

    private fun mergeWithWeight(another: CBM): CBM {
        val inputWeight = another.weight
        val inputMulti = BMUtils.minMultiToInt(inputWeight)
        val currentWeightMulti = BMUtils.minMultiToInt(weight)
        val commonMulti = max(inputMulti, currentWeightMulti)

        val weightInt = (weight * commonMulti).toInt()
        val inputWeightInt = (inputWeight * commonMulti).toInt()
        val commonWeightFactor = BMUtils.commonFactor(weightInt, inputWeightInt)

        val weightFactor = weightInt / commonWeightFactor
        val inputWeightFactor = inputWeightInt / commonWeightFactor

        val cbm = CBM(another.container, false, inputWeight)
        container.clear()
        container.putAll(multiBy(this, weightFactor).container)
        mergeWithoutWeight(multiBy(cbm, inputWeightFactor))

        weight /= weightFactor
        return this
    }

    private fun mergeWithoutWeight(another: CBM): CBM {
        val max1 = container.keys.maxOrNull() ?: 0
        val max2 = another.container.keys.maxOrNull() ?: 0
        val maxPos = max(max1, max2)
        mergeHelper(another, 0, BBM(), maxPos)
        return this
    }

    private fun mergeHelper(another: CBM, pos: Int, carry: BBM, maxPos: Int): CBM {
        if (pos <= maxPos || !carry.isEmpty()) {
            val a = container[pos] ?: BBM()
            val b = another.container[pos] ?: BBM()

            /*
             * the following lines means: (b & (a | c)) | (a & c)
             * equivalent to: (a & b) | ( b & c) | (a & c)
             */
            val newCarry = BBM.or(
                BBM.and(
                    b,
                    BBM.or(a, carry)
                ),
                BBM.and(a, carry)
            )
            a.xor(b)
            a.xor(carry)
            if (!container.containsKey(pos) && !a.isEmpty()) {
                container[pos] = a
            }
            return mergeHelper(another, pos + 1, newCarry, maxPos)
        } else {
            return this
        }
    }

    override fun xor(bm: BM): CBM = resetModify {
        when (bm) {
            is RBM, is BBM -> {
                container.values.forEach { it.xor(bm) }
            }
            else -> throw IllegalArgumentException()
        }
        updateMaxBit()
        this
    }

    fun hasWeight(): Boolean {
        return abs(weight - 1) > 1e-6
    }

    fun getTopCount(n: Int) = getTopCount(n, true)
    fun getTopCount(n: Int, desc: Boolean): Map<Int, Double> {
        if (n >= getCountUnique()) {
            return getAllCount()
        }
        val countDistribution = getDistribution()
        var maxCount = 0.0
        var minCount = 0.0
        countDistribution.keys.forEach {
            maxCount = max(maxCount, it)
            minCount = min(minCount, it)
        }

        var (currentCount, step) = if (desc) Pair(maxCount, -1) else Pair(minCount, 1)
        val topCount = hashMapOf<Int, Double>()
        var currentUCount = 0.0
        while (currentCount in minCount..maxCount && currentUCount < n) {
            val currentCountRBM = countDistribution[currentCount] ?: RBM()
            val ulist = currentCountRBM.iterator()
            while (ulist.hasNext() && currentUCount < n) {
                topCount[ulist.next()] = currentCount
                currentUCount += 1
            }
            currentCount += step
        }
        return topCount
    }
    fun getAllCount(): Map<Int, Double> {
        val counts = hashMapOf<Int, Double>()
        container.forEach { (bit, bbm) ->
            val bitCount = weight * (1 shl bit)
            bbm.container.forEach { (_, rbm) ->
                rbm.forEach {
                    val currentCount = counts[it] ?: 0.0
                    counts[it] = currentCount + bitCount
                }
            }
        }
        return counts
    }

    fun getDistribution() = getDistribution(Long.MAX_VALUE)
    fun getDistribution(maxCount: Long): Map<Double, RBM> {
        val realMaxCount = (maxCount / weight).toLong()
        val rCBM = mergeAsRBM(this)
        val distributions = hashMapOf<Long, RBM>()
        rCBM.container.forEach { (bit, bm) ->
            val currentCnt = 1 shl bit
            val currentRBM = bm.getRBM()
            val newCnts = HashMap<Long, RBM>()
            val maxCountRBM = distributions[realMaxCount] ?: RBM()
            distributions.forEach { (cnt, rbm) ->
                val inc = RBM.and(rbm, currentRBM)
                currentRBM.andNot(inc)
                if (cnt == realMaxCount) {
                    maxCountRBM.or(inc)
                } else {
                    rbm.andNot(inc)
                    val totalCnt = cnt + currentCnt
                    if (totalCnt >= realMaxCount) {
                        maxCountRBM.or(inc)
                    } else {
                        newCnts[totalCnt] = inc
                    }
                }
            }
            newCnts.forEach { (key, value) -> distributions[key] = value }
            if (currentRBM.getLongCount() != 0L) {
                if (currentCnt >= realMaxCount) {
                    maxCountRBM.or(currentRBM)
                } else {
                    distributions[currentCnt.toLong()] = currentRBM
                }
            }

            if (maxCountRBM.getLongCount() != 0L) {
                distributions[realMaxCount] = maxCountRBM
            }
            distributions.values.removeIf { it.isEmpty() }
        }
        return distributions
            .filter { it.value.getLongCount() > 0L }
            .mapKeys { weight * it.key }
    }

    /**
     * [ComparableBM]
     */
    override fun gt(threshold: Double): CBM = this.gt(threshold, false)
    override fun gt(threshold: Double, copy: Boolean): CBM {
        return filterWithThreshold(this, threshold, true, false, copy)
    }
    override fun gte(threshold: Double): CBM = this.gte(threshold, false)
    override fun gte(threshold: Double, copy: Boolean): CBM {
        return filterWithThreshold(this, threshold, true, true, copy)
    }
    override fun lt(threshold: Double): CBM = this.lt(threshold, false)
    override fun lt(threshold: Double, copy: Boolean): CBM {
        return filterWithThreshold(this, threshold, false, false, copy)
    }
    override fun lte(threshold: Double): CBM = this.lte(threshold, false)
    override fun lte(threshold: Double, copy: Boolean): CBM {
        return filterWithThreshold(this, threshold, false, true, copy)
    }
    override fun eq(threshold: Double): CBM = this.eq(threshold, false)
    override fun eq(threshold: Double, copy: Boolean): CBM {
        return equalsOrNot(this, threshold, copy, true)
    }
    override fun neq(threshold: Double): CBM = this.neq(threshold, false)
    override fun neq(threshold: Double, copy: Boolean): CBM {
        return equalsOrNot(this, threshold, copy, false)
    }
    override fun between(first: Double, second: Double): CBM = this.between(first, second, false)
    override fun between(first: Double, second: Double, copy: Boolean): CBM {
        return when {
            first > second -> CBM()
            first == second -> this.eq(first)
            else -> this.gte(first, copy).lte(second)
        }
    }

    private fun filterWithThreshold(cbm: CBM, threshold: Double, greater: Boolean, equals: Boolean, copy: Boolean): CBM {
        var maxCount = Long.MAX_VALUE
        if (!greater) {
            maxCount = min(threshold.toLong() + 1, maxCount)
        }
        val distribution = cbm.getDistribution(maxCount)
        val rbm = RBM()
        distribution.forEach { (currentCnt, r) ->
            if ((greater && currentCnt > threshold) ||
                (!greater && currentCnt < threshold) ||
                (equals && abs(currentCnt - threshold) < 1e-6)
            ) {
                rbm.or(r)
            }
        }
        return CBM(cbm.container, copy, cbm.weight).and(rbm)
    }

    private fun equalsOrNot(cbm: CBM, threshold: Double, copy: Boolean, equals: Boolean): CBM {
        val cnt = (threshold / cbm.weight).toLong()
        if (cnt <= 0 || abs(cnt * cbm.weight - threshold) >= 1e-6) {
            return CBM()
        }
        val roaringCBM = mergeAsRBM(cbm)
        val bits: IntArray = BMUtils.oneBitPositions(cnt)
        val i1Max = bits.size
        val i2Max = roaringCBM.maxBit + 1
        if (bits[i1Max - 1] >= i2Max) {
            return CBM()
        }
        val rbm = roaringCBM.container.getOrDefault(bits[0], BBM()).getRBM()
        var i1 = 0
        var i2 = 0

        while (i1 < i1Max && i2 < i2Max) {
            val b1 = bits[i1]
            val rbm2 = roaringCBM.container.getOrDefault(i2, BBM()).getRBM()
            if (b1 == i2) {
                rbm.and(rbm2)
                i1 += 1
                i2 += 1
            } else {
                while (b1 > i2 && i2 < i2Max) {
                    rbm.andNot(rbm2)
                    i2 += 1
                }
            }
        }

        while (i2 < i2Max) {
            rbm.andNot(roaringCBM.container.getOrDefault(i2, BBM()).getRBM())
            i2 += 1
        }

        if (rbm.isEmpty()) {
            return CBM()
        }
        val result = CBM(cbm.container, copy, cbm.weight)
        return if (equals) result.and(rbm) else result.andNot(rbm)
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

        @JvmStatic
        fun and(cbm1: CBM, bm2: RBM): CBM = cbm1.clone().and(bm2)
        @JvmStatic
        fun and(cbm1: CBM, bm2: BBM): CBM = cbm1.clone().and(bm2)

        @JvmStatic
        fun andNot(cbm1: CBM, bm2: RBM): CBM = cbm1.clone().andNot(bm2)
        @JvmStatic
        fun andNot(cbm1: CBM, bm2: BBM): CBM = cbm1.clone().andNot(bm2)

        @JvmStatic
        fun or(cbm1: CBM, cbm2: CBM): CBM = cbm1.clone().or(cbm2)
        @JvmStatic
        fun or(vararg cbms: CBM): CBM {
            if (cbms.isEmpty()) return CBM()
            if (cbms.size == 1) return cbms[0]
            val hasWeight = cbms.map { it.hasWeight() }.any { it }
            if (hasWeight) {
                return cbms.fold(CBM()) { a, b -> a.or(b) }
            }
            // TODO (replace with fast way)
            return cbms.fold(CBM()) { a, b -> a.or(b) }
        }

        @JvmStatic
        fun gt(cbm: CBM, threshold: Double): CBM = cbm.gt(threshold, true)
        @JvmStatic
        fun gte(cbm: CBM, threshold: Double): CBM = cbm.gte(threshold, true)
        @JvmStatic
        fun lt(cbm: CBM, threshold: Double): CBM = cbm.lt(threshold, true)
        @JvmStatic
        fun lte(cbm: CBM, threshold: Double): CBM = cbm.lte(threshold, true)
        @JvmStatic
        fun eq(cbm: CBM, threshold: Double): CBM = cbm.eq(threshold, true)
        @JvmStatic
        fun neq(cbm: CBM, threshold: Double): CBM = cbm.neq(threshold, true)
        @JvmStatic
        fun between(cbm: CBM, first: Double, second: Double): CBM = cbm.between(first, second, true)

        @JvmStatic
        fun mergeAsRBM(cbm: CBM): CBM {
            val bucketIds = cbm.container.values.flatMap { it.container.keys }.distinct().sorted()
            val cbms = mutableListOf<CBM>()
            bucketIds.forEach { bid ->
                val tempCBMContainer = hashMapOf<Int, BBM>()
                cbm.container.forEach { (k, v) ->
                    val tempBBMContainer = hashMapOf<Int, RBM>()
                    tempBBMContainer[BBM.MAGIC_BUCKET] = v.container[bid] ?: RBM()
                    tempCBMContainer[k] = BBM(tempBBMContainer, true)
                }
                cbms.add(CBM(tempCBMContainer, true))
            }
            val result = or(*cbms.toTypedArray())
            result.weight = cbm.weight
            return result
        }

        @JvmStatic
        fun multiBy(cbm: CBM, multi: Int): CBM {
            PreConditions.checkExpression(multi >= 0)
            val result = CBM()
            BMUtils.oneBitPositions(multi.toLong()).forEach { bit ->
                result.mergeWithoutWeight(shift(cbm, bit))
            }
            result.weight = cbm.weight
            return result
        }

        @JvmStatic
        fun shift(cbm: CBM, bit: Int): CBM {
            val results = hashMapOf<Int, BBM>()
            cbm.container.forEach { (k, v) ->
                val shiftedBit = k + bit
                if (shiftedBit >= 0) {
                    results[shiftedBit] = v
                }
            }
            return CBM(results, true, cbm.weight)
        }
    }
}
