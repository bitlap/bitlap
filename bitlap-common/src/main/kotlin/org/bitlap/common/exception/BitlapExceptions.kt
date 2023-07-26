/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.exception

/**
 * Desc: bitlap exceptions builder
 */
object BitlapExceptions {

    @JvmStatic
    fun illegalException(argument: String) =
        BitlapIllegalArgumentException("ERR_ILLEGAL", mapOf("argument" to argument))

    @JvmStatic
    fun checkNotNullException(argument: String) =
        BitlapNullPointerException("ERR_ILLEGAL.CHECK_NOT_NULL", mapOf("argument" to argument))

    @JvmStatic
    fun checkNotEmptyException(argument: String) =
        BitlapIllegalArgumentException("ERR_ILLEGAL.CHECK_NOT_EMPTY", mapOf("argument" to argument))

    @JvmStatic
    fun checkNotBlankException(argument: String) =
        BitlapIllegalArgumentException("ERR_ILLEGAL.CHECK_NOT_BLANK", mapOf("argument" to argument))
}
