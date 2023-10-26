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

/**
 * bitlap exceptions builder
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
