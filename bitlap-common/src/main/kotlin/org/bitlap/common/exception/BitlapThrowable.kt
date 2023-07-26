/* Copyright (c) 2023 bitlap.org */
package org.bitlap.common.exception

/**
 * Desc: Common bitlap throwable
 */
interface BitlapThrowable {

    /**
     * the key for the error message, it must be unique.
     * if the error message of the key is null, the key will be the default error message.
     */
    val errorKey: String

    /**
     * the parameters for the error message
     */
    val parameters: Map<String, String>

    /**
     * sql state
     */
    fun sqlState(): String = this.errorKey.formatSqlState()
}
