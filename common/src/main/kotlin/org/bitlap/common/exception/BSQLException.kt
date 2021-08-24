package org.bitlap.common.exception

/**
 * driver exception
 * please use this when we do not need error stack.
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
data class BSQLException(val msg: String = "SQL Exception") : BitlapException(msg)
