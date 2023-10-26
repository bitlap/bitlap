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

import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.spec.style.StringSpec
import org.bitlap.common.exception.BitlapExceptions
import org.bitlap.common.exception.BitlapIllegalArgumentException
import org.bitlap.common.exception.BitlapNullPointerException

/**
 * test bitlap exceptions
 */
class BitlapExceptionTest : StringSpec({

    "test simple exception" {
        // error key not exist
        shouldThrowWithMessage<BitlapNullPointerException>("xxxx") {
            throw BitlapNullPointerException("xxxx", emptyMap())
        }
        // error key exists
        shouldThrowWithMessage<BitlapIllegalArgumentException>("arg1 is illegal.") {
            throw BitlapExceptions.illegalException("arg1")
        }
        shouldThrowWithMessage<BitlapNullPointerException>("arg1 cannot be null.") {
            throw BitlapExceptions.checkNotNullException("arg1")
        }
    }
})
