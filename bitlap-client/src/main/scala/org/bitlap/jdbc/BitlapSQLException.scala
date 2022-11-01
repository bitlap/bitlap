/* Copyright (c) 2022 bitlap.org */
package org.bitlap.jdbc

import java.sql.SQLException

/** bitlap 客户端的SQL异常
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
final case class BitlapSQLException(
  msg: String,
  state: String = null,
  code: Int = -1,
  cause: Throwable = null
) extends SQLException(msg, state, code, cause)
