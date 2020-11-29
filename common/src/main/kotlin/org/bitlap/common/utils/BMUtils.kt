package org.bitlap.common.utils

import org.bitlap.common.bitmap.BM
import org.bitlap.common.bitmap.RBM
import org.bitlap.common.bitmap.rbm.RoaringBitmap

/**
 * Desc: [BM] Utils
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/21
 */
object BMUtils {

    inline fun <reified T> or(vararg bms: T): T where T : BM = or(bms.toList())
    inline fun <reified T> or(bms: Collection<BM>): T where T : BM {
        return when (T::class.java) {
            RBM::class.java -> {
                RBM(RoaringBitmap.or(bms.map { (it as RBM).getNativeRBM() }.iterator()))
            }
            else -> {
                throw IllegalArgumentException()
            }
        } as T
    }

}