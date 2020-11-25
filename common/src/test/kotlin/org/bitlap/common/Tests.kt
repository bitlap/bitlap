package org.bitlap.common

import org.bitlap.common.bitmap.RBM

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */
fun main() {

    val rbm = RBM(1, 10000000)
    println(rbm.getCount())
    println(RBM(rbm.getBytes()).getCount())

    val splits = rbm.split(10)
    println(splits.size)
    val rbmR = splits.values.fold(RBM()) { a, b ->
        a.or(b)
    }
    println(rbmR.getCount())
}
