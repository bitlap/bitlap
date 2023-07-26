/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.test

import io.kotest.assertions.throwables.shouldThrowWithMessage
import io.kotest.core.spec.style.StringSpec
import org.bitlap.common.exception.BitlapExceptions
import org.bitlap.common.exception.BitlapIllegalArgumentException
import org.bitlap.common.exception.BitlapNullPointerException

/**
 * Desc: test bitlap exceptions
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
