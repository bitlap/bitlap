package org.bitlap.common.test.bitmap

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.common.test.utils.BMTestUtils

/**
 * Desc: [BM] Test
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/4
 */
class BMTest : StringSpec({

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

    "Simple CBM with 1000 ids" {
        val cbm = CBM()
        cbm.add(0, 1, 10)
        cbm.add(0, 2, 20)
        cbm.add(0, 3, 30)
        cbm.add(0, 4, 20)
        cbm.add(1, 1, 40)

        cbm.getCount() shouldBe 120
        cbm.getCountUnique() shouldBe 4
        cbm.getDistribution()[20.0]!!.getCountUnique() shouldBe 2
        cbm.getTopCount(2).keys shouldBe setOf(1, 3)
        CBM.eq(cbm, 20.0).getRBM().toList() shouldBe listOf(2, 4)
        CBM.neq(cbm, 20.0).getRBM().toList() shouldBe listOf(1, 3)
        CBM.gte(cbm, 30.0).getRBM().toList() shouldBe listOf(1, 3)
        CBM.gt(cbm, 30.0).getRBM().toList() shouldBe listOf(1)
        CBM.lte(cbm, 30.0).getRBM().toList() shouldBe listOf(2, 3, 4)
        CBM.lt(cbm, 30.0).getRBM().toList() shouldBe listOf(2, 4)
        CBM.between(cbm, 20.0, 30.0).getRBM().toList() shouldBe listOf(2, 3, 4)

        // test weight
        val cbmW = cbm.clone()
        cbmW.weight = 0.1
        cbmW.getCount() shouldBe 12
        cbmW.getCountUnique() shouldBe 4
        cbmW.getDistribution()[2.0]!!.getCountUnique() shouldBe 2
        cbmW.getTopCount(2).keys shouldBe setOf(1, 3)
        CBM.eq(cbmW, 2.0).getRBM().toList() shouldBe listOf(2, 4)
        CBM.neq(cbmW, 2.0).getRBM().toList() shouldBe listOf(1, 3)
        CBM.gte(cbmW, 3.0).getRBM().toList() shouldBe listOf(1, 3)
        CBM.gt(cbmW, 3.0).getRBM().toList() shouldBe listOf(1)
        CBM.lte(cbmW, 3.0).getRBM().toList() shouldBe listOf(2, 3, 4)
        CBM.lt(cbmW, 3.0).getRBM().toList() shouldBe listOf(2, 4)
        CBM.between(cbmW, 2.0, 3.0).getRBM().toList() shouldBe listOf(2, 3, 4)
    }

    "CBM with 2000000 ids" {
        val num = 2000000
        val cbm = BMTestUtils.randomCBM(100000, 10, num, num)
        // serialize
        val cbmSerD = CBM(cbm.getBytes())
        cbm.getCount() shouldBe cbmSerD.getCount()
        cbm.getCountUnique() shouldBe cbmSerD.getCountUnique()
        // split
        val cbmSplit = cbm.split(10, true).values.fold(CBM()) { r1, r2 ->
            r1.or(r2)
        }
        cbm.getCount() shouldBe cbmSplit.getCount()
        cbm.getCountUnique() shouldBe cbmSplit.getCountUnique()
    }
})
