package org.bitlap.common

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.common.bitmap.or

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/16
 */
fun main2() {

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

fun main() {

    val bbm = BBM(mapOf(
        1 to RBM(1, 1000000),
        2 to RBM(1, 1000000)
    ))
    println(BBM(bbm.getBytes()).getCount())

    val splits = bbm.split(4)
    println(splits.size)
    val bbmR = splits.values.fold(BBM()) { a, b ->
        a.or(b)
    }
    println(bbmR.getCount())
}