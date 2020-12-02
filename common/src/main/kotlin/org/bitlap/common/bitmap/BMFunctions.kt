@file:Suppress("UNCHECKED_CAST")

package org.bitlap.common.bitmap

/**
 * Desc: Bitmap enhance functions
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/26
 */

fun <T1, T2> T1.and(bm: T2): T1 where T1 : BM, T2: BM {
    return when (this) {
        is RBM -> this._and(bm.getRBM())
        is BBM -> {
            when (bm) {
                is RBM -> this._and(bm)
                is BBM -> this._and(bm)
                else -> throw IllegalArgumentException()
            }
        }
        else -> {
            throw IllegalArgumentException()
        }
    } as T1
}

fun <T> T.andNot(bm: T): T where T : BM {
    return when (this) {
        is RBM -> this._andNot(bm.getRBM())
        is BBM -> {
            when (bm) {
                is RBM -> this._andNot(bm)
                is BBM -> this._andNot(bm)
                else -> throw IllegalArgumentException()
            }
        }
        else -> {
            throw IllegalArgumentException()
        }
    } as T
}

fun <T> T.or(bm: T): T where T : BM {
    return when (this) {
        is RBM -> this._or(bm.getRBM())
        is BBM -> {
            when (bm) {
                is RBM -> this._or(bm)
                is BBM -> this._or(bm)
                else -> throw IllegalArgumentException()
            }
        }
        else -> {
            throw IllegalArgumentException()
        }
    } as T
}

fun <T> T.xor(bm: T): T where T : BM {
    return when (this) {
        is RBM -> this._xor(bm.getRBM())
        is BBM -> {
            when (bm) {
                is RBM -> this._xor(bm)
                is BBM -> this._xor(bm)
                else -> throw IllegalArgumentException()
            }
        }
        else -> {
            throw IllegalArgumentException()
        }
    } as T
}
