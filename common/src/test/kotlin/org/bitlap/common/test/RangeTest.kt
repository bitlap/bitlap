package org.bitlap.common.test

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.utils.Range

/**
 * Desc: [Range] test
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/6/21
 */
class RangeTest : StringSpec({

    "test simple range utils" {
        val openRange = Range.open(2, 10)
        (1 in openRange) shouldBe false
        (2 in openRange) shouldBe false
        (3 in openRange) shouldBe true
        (5 in openRange) shouldBe true
        (10 in openRange) shouldBe false
        (11 in openRange) shouldBe false

        val closedRange = Range.closed(2, 10)
        (1 in closedRange) shouldBe false
        (2 in closedRange) shouldBe true
        (3 in closedRange) shouldBe true
        (5 in closedRange) shouldBe true
        (10 in closedRange) shouldBe true
        (11 in closedRange) shouldBe false
    }

})
