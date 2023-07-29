/**
 * Copyright (C) 2023 bitlap.org .
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
