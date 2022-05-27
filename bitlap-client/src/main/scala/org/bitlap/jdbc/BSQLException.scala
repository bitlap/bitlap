/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import java.sql.SQLException

/** @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
case class BSQLException(
  msg: String,
  state: String = null,
  code: Int = -1,
  cause: Throwable = null
) extends SQLException(msg, state, code, cause)
