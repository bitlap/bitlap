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

import org.bitlap.common.exception.BitlapExceptions
import org.bitlap.common.exception.BitlapNullPointerException

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should

/** Desc: test bitlap exceptions
 */
class BitlapExceptionTest extends AnyFunSuite with should.Matchers {

  test("test simple exception") {
    val e1 = BitlapNullPointerException("xxxx", Map.empty)
    e1.getMessage shouldBe "xxxx"

    val e2 = BitlapExceptions.illegalException("arg1")
    e2.getMessage shouldBe "arg1 is illegal."

    val e3 = BitlapExceptions.checkNotNullException("arg1")
    e3.getMessage shouldBe "arg1 cannot be null."
  }
}
