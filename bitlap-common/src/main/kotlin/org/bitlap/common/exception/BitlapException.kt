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
