package org.bitlap.server.raft.cli

import org.bitlap.common.exception.BitlapException

/**
 * driver exception
 *
 * @author 梦境迷离
 * @since 2021/6/6
 * @version 1.0
 */
data class BSQLException(val msg: String = "SQL Exception") : BitlapException(msg)
