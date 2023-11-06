/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.common.test

/*

import org.bitlap.common.utils.Range
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

/**
 * [Range] test
 */
class RangeTest extends AnyFunSuite with should.Matchers {

    test("test simple range utils") {
        val single = Range.singleton(3)
        (2 in single) shouldBe false
        (3 in single) shouldBe true
        (4 in single) shouldBe false
        single.isEmpty() shouldBe false

        val openRange = Range.open(2, 10)
        (1 in openRange) shouldBe false
        (2 in openRange) shouldBe false
        (3 in openRange) shouldBe true
        (5 in openRange) shouldBe true
        (10 in openRange) shouldBe false
        (11 in openRange) shouldBe false
        openRange.isEmpty() shouldBe false

        val closedRange = Range.closed(2, 10)
        (1 in closedRange) shouldBe false
        (2 in closedRange) shouldBe true
        (3 in closedRange) shouldBe true
        (5 in closedRange) shouldBe true
        (10 in closedRange) shouldBe true
        (11 in closedRange) shouldBe false
        closedRange.isEmpty() shouldBe false

        val lessThan = Range.lessThan(10)
        (1 in lessThan) shouldBe true
        (10 in lessThan) shouldBe false
        (11 in lessThan) shouldBe false
        lessThan.isEmpty() shouldBe false

        val atMost = Range.atMost(10)
        (1 in atMost) shouldBe true
        (10 in atMost) shouldBe true
        (11 in atMost) shouldBe false
        atMost.isEmpty() shouldBe false

        val greaterThan = Range.greaterThan(2)
        (1 in greaterThan) shouldBe false
        (2 in greaterThan) shouldBe false
        (3 in greaterThan) shouldBe true
        greaterThan.isEmpty() shouldBe false

        val atLeast = Range.atLeast(2)
        (1 in atLeast) shouldBe false
        (2 in atLeast) shouldBe true
        (3 in atLeast) shouldBe true
        atLeast.isEmpty() shouldBe false

        val all = Range.all<Int>()
        all.contains(10) shouldBe true
        all.isEmpty() shouldBe false

        var empty = Range.open(2, 2)
        empty.isEmpty() shouldBe true
        empty = Range.closed(3, 2)
        empty.isEmpty() shouldBe true
        empty = Range.openClosed(2, 2)
        empty.isEmpty() shouldBe true
        empty = Range.closedOpen(2, 2)
        empty.isEmpty() shouldBe true
    }
}

 */
