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

import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.bitlap.common.utils.BMUtils

class BMUtilsTest : StringSpec({

    "test min multiply to int" {
        BMUtils.minMultiToInt(1.0) shouldBe 1
        BMUtils.minMultiToInt(100.0) shouldBe 1
        BMUtils.minMultiToInt(0.1) shouldBe 10
        BMUtils.minMultiToInt(0.01) shouldBe 100
        BMUtils.minMultiToInt(0.0000001) shouldBe 1 // overflow
    }
})
