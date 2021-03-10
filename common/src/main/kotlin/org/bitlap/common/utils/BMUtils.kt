package org.bitlap.common.utils

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.BM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.common.bitmap.Versions
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import kotlin.math.max
import kotlin.math.min

/**
 * Desc: [BM] Utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/21
 */
object BMUtils {

    /**
     * Bitmaps aggregate with [or] function, [T] should be the same implement.
     */
    @JvmStatic
    fun javaOr(vararg bms: BM): BM = or(*bms)
    inline fun <reified T> or(vararg bms: T): T where T : BM = or(bms.toList())
    inline fun <reified T> or(bms: Collection<T>): T where T : BM {
        if (bms.isEmpty()) {
            return T::class.java.newInstance()
        }
        if (bms.size == 1) {
            return bms.first().clone() as T
        }
        if (bms.size == 2) {
            return or(bms.first(), bms.last())
        }
        return when (T::class.java) {
            RBM::class.java -> RBM.or(*bms.map { it as RBM }.toTypedArray())
            BBM::class.java -> BBM.or(*bms.map { it as BBM }.toTypedArray())
            CBM::class.java -> CBM.or(*bms.map { it as CBM }.toTypedArray())
            else -> throw IllegalArgumentException()
        } as T
    }

    /**
     * Bitmaps aggregate with [and] function, [T] should be the same implement.
     */
    @JvmStatic
    fun javaAnd(vararg bms: BM) = and(*bms)
    inline fun <reified T> and(vararg bms: T): T where T : BM = and(bms.toList())
    inline fun <reified T> and(bms: Collection<T>): T where T : BM {
        if (bms.isEmpty()) {
            return T::class.java.newInstance()
        }
        if (bms.size == 1) {
            return bms.first().clone() as T
        }
        if (bms.size == 2) {
            return and(bms.first(), bms.last())
        }
        return when (T::class.java) {
            RBM::class.java -> RBM.and(*bms.map { it as RBM }.toTypedArray())
            BBM::class.java -> BBM.and(*bms.map { it as BBM }.toTypedArray())
            else -> throw IllegalArgumentException()
        } as T
    }

    /**
     * Two Bitmap [or] function, [T1] can be different implement
     */
    inline fun <reified T1, reified T2> or(bm1: T1, bm2: T2): T1 where T1 : BM, T2 : BM {
        return when (T1::class.java) {
            RBM::class.java -> (bm1 as RBM).clone().or(bm2)
            BBM::class.java -> (bm1 as BBM).clone().or(bm2)
            CBM::class.java -> (bm1 as CBM).clone().or(bm2)
            else -> throw IllegalArgumentException()
        } as T1
    }

    /**
     * Two Bitmap [and] function, [T1] can be different implement
     */
    inline fun <reified T1, reified T2> and(bm1: T1, bm2: T2): T1 where T1 : BM, T2 : BM {
        return when (T1::class.java) {
            RBM::class.java -> (bm1 as RBM).clone().and(bm2)
            BBM::class.java -> (bm1 as BBM).clone().and(bm2)
            CBM::class.java -> (bm1 as CBM).clone().and(bm2)
            else -> throw IllegalArgumentException()
        } as T1
    }

    /**
     * Two Bitmap [andNot] function, [T1] can be different implement
     */
    inline fun <reified T1, reified T2> andNot(bm1: T1, bm2: T2): T1 where T1 : BM, T2 : BM {
        return when (T1::class.java) {
            RBM::class.java -> (bm1 as RBM).clone().andNot(bm2)
            BBM::class.java -> (bm1 as BBM).clone().andNot(bm2)
            CBM::class.java -> (bm1 as CBM).clone().andNot(bm2)
            else -> throw IllegalArgumentException()
        } as T1
    }

    /**
     * Two Bitmap [xor] function, [T1] can be different implement
     */
    inline fun <reified T1, reified T2> xor(bm1: T1, bm2: T2): T1 where T1 : BM, T2 : BM {
        return when (T1::class.java) {
            RBM::class.java -> (bm1 as RBM).clone().xor(bm2)
            BBM::class.java -> (bm1 as BBM).clone().xor(bm2)
            CBM::class.java -> (bm1 as CBM).clone().xor(bm2)
            else -> throw IllegalArgumentException()
        } as T1
    }

    /**
     * @return the positions of 1-bit in the long value
     */
    @JvmStatic
    fun oneBitPositions(count: Long): IntArray {
        var cnt = count
        val bits = IntArray(cnt.countOneBits())
        var i = 0
        while (cnt > 0) {
            val b = cnt and -cnt
            bits[i++] = (b - 1).countOneBits()
            cnt -= b
        }
        return bits
    }

    /**
     * get common factor between i1 and i2
     */
    @JvmStatic
    fun commonFactor(i1: Int, i2: Int): Int = when {
        i1 == 0 -> {
            i2
        }
        i2 == 0 -> {
            i1
        }
        else -> {
            var f1 = max(i1, i2)
            var f2 = min(i1, i2)
            while (f1 % f2 != 0) {
                val tmp = f1 % f2
                f1 = f2
                f2 = tmp
            }
            f2
        }
    }

    /**
     * @return min multiply to Int
     */
    @JvmStatic
    fun minMultiToInt(input: Double): Int = when {
        input < 1e-6 -> {
            1
        }
        else -> {
            var currentFactor = 1
            var currentCnt = input * currentFactor
            while ((currentCnt - currentCnt.toInt()) / currentCnt > 0.01) {
                currentFactor *= 10
                currentCnt = input * currentFactor
            }
            currentFactor
        }
    }

    @JvmStatic
    fun fromBytes(bytes: ByteArray?, defaultValue: BM? = null): BM? {
        if (bytes == null || bytes.isEmpty()) {
            return defaultValue
        }
        val dis = DataInputStream(ByteArrayInputStream(bytes))
        return when (dis.use { it.readInt() }) {
            Versions.RBM_VERSION_V1 -> RBM(bytes)
            Versions.BBM_VERSION_V1 -> BBM(bytes)
            Versions.CBM_VERSION_V1 -> CBM(bytes)
            else -> defaultValue
        }
    }

    /**
     * Compute [CBM] with op
     */
    @JvmStatic
    fun compute(cbm: CBM, op: String, threshold: Array<Double>): CBM {
        if (threshold.isEmpty()) {
            throw IllegalArgumentException("Illegal threshold: $threshold, op: $op")
        }
        return when (op) {
            ">=" -> CBM.gte(cbm, threshold.first())
            ">" -> CBM.gt(cbm, threshold.first())
            "<=" -> CBM.lte(cbm, threshold.first())
            "<" -> CBM.lt(cbm, threshold.first())
            "=" -> CBM.equals(cbm, threshold.first())
            "!=" -> CBM.andNot(cbm, CBM.equals(cbm, threshold.first()).getRBM())
            "between" -> {
                if (threshold.size <= 1) {
                    throw IllegalArgumentException("Illegal threshold: $threshold, op: $op")
                }
                val (first, second) = threshold
                when {
                    first > second -> CBM()
                    first == second -> CBM.equals(cbm, first)
                    else -> CBM.lte(CBM.gte(cbm, first), second)
                }
            }
            else -> throw IllegalArgumentException("Illegal threshold: $threshold, op: $op")
        }
    }
}
