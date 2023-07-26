/* Copyright (c) 2023 bitlap.org */
package org.bitlap.jdbc

import java.sql.SQLException

import scala.jdk.CollectionConverters.*

import org.bitlap.common.exception.BitlapException

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
  cause: Option[Throwable] = None)
    extends SQLException(msg, state, code, cause.orNull)

final case class BitlapJdbcUriParseException(msg: String, cause: Option[Throwable] = None)
    extends BitlapException(msg, Map.empty[String, String].asJava, cause.orNull)
