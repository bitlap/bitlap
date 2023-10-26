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
package org.bitlap.common.utils

import kotlin.random.Random

/**
 * Random extension utils
 */
object RandomEx {

    private const val BASE_NUMBER = "0123456789"
    private const val BASE_CHAR = "abcdefghijklmnopqrstuvwxyz"
    private const val BASE_CHAR_NUMBER = BASE_CHAR + BASE_NUMBER

    @JvmStatic
    fun string(limit: Int): String {
        PreConditions.checkExpression(limit > 0)
        return (0 until limit)
            .map { BASE_CHAR_NUMBER[Random.nextInt(BASE_CHAR_NUMBER.length)] }
            .joinToString("")
    }
}
