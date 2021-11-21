package org.bitlap.net

import java.sql.SQLException

/**
 *
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
case class BSQLException(msg: String = "Bitlap SQL Exception", cause: Throwable = null) extends SQLException(msg, cause)
