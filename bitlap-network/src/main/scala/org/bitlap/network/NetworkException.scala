/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

/**
 * @author 梦境迷离
 * @since 2021/11/20
 * @version 1.0
 */
case class NetworkException(
  code: Int,
  msg: String = "",
  cause: Throwable = null
) extends Throwable(msg, cause)
