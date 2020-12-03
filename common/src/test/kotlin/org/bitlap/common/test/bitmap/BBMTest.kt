package org.bitlap.common.test.bitmap

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.common.bitmap.or
import org.bitlap.common.test.utils.BMTestUtils

/**
 * Desc: [BBM] Test
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/4
 */
class BBMTest : StringSpec({

    "RBM with 1000 ids" {
        val num = 1000
        // serialize
        val rbm = BMTestUtils.randomRBM(num, max = Int.MAX_VALUE)
        rbm.getCount() shouldBe num
        rbm.getCountUnique() shouldBe num
        val newRBM = RBM(rbm.getBytes())
        newRBM.getCount() shouldBe num
        // split
        val splits = rbm.split(2)
        splits.size shouldBe 2
        val sRBM = splits.values.fold(RBM()) { r1, r2 ->
            r1.or(r2)
        }
        sRBM.getCount() shouldBe num
    }

    "BBM with 1000 ids" {
        val num = 1000
        // serialize
        val bbm = BMTestUtils.randomBBM(10, num, max = Int.MAX_VALUE)
        bbm.getCount() shouldBe num
        bbm.getCountUnique() shouldBe num
        val newBBM = BBM(bbm.getBytes())
        newBBM.getCountUnique() shouldBe num
        // split
        val splits = bbm.split(2)
        splits.size shouldBe 2
        val sBBM = splits.values.fold(BBM()) { r1, r2 ->
            r1.or(r2)
        }
        sBBM.getCountUnique() shouldBe num
    }
})