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

import org.bitlap.common.BitlapIterator

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

class BitlapIteratorTest extends AnyFunSuite with should.Matchers {

  test("Test simple BitlapIterator") {
    val iterator = BitlapIterator.of(1 to 100)
    iterator.sum shouldBe 5050
    val batchIterator = BitlapIterator.batch(1 to 100, 10)
    batchIterator.toList.map { _ * 2 }.sum shouldBe 5050 * 2
  }
}
