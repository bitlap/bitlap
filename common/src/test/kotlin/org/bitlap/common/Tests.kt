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

    val rbm = RBM()
    rbm.add(1)
    rbm.add(2)
    rbm.add(3)

    println(rbm.getCardinality())
}
