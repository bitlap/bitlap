/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.exception

import java.io.IOException

open class BitlapException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, Exception(errorKey.formatErrorMessage(parameters), cause) {
    companion object {
    }
}

open class BitlapRuntimeException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, RuntimeException(errorKey.formatErrorMessage(parameters), cause)

open class BitlapNullPointerException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, NullPointerException(errorKey.formatErrorMessage(parameters))

open class BitlapIllegalArgumentException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, IllegalArgumentException(errorKey.formatErrorMessage(parameters), cause)

open class BitlapIllegalStateException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, IllegalStateException(errorKey.formatErrorMessage(parameters), cause)

open class BitlapSQLException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, RuntimeException(errorKey.formatErrorMessage(parameters), cause)

open class BitlapIOException @JvmOverloads constructor(
    override val errorKey: String,
    override val parameters: Map<String, String> = emptyMap(),
    override val cause: Throwable? = null
) : BitlapThrowable, IOException(errorKey.formatErrorMessage(parameters), cause)
