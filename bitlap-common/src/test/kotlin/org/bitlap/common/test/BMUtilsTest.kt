/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.utils.BMUtils

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/5
 */
class BMUtilsTest : StringSpec({

    "test min multiply to int" {
        BMUtils.minMultiToInt(1.0) shouldBe 1
        BMUtils.minMultiToInt(100.0) shouldBe 1
        BMUtils.minMultiToInt(0.1) shouldBe 10
        BMUtils.minMultiToInt(0.01) shouldBe 100
        BMUtils.minMultiToInt(0.0000001) shouldBe 1 // overflow
    }
})
