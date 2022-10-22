/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

import org.bitlap.common.exception.BitlapException

/** All exceptions for network and server.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed abstract class BitlapNetworkException(
  val code: Int = -1,
  val msg: String,
  val cause: Option[Throwable] = None
) extends BitlapException(msg, cause.orNull)
    with Product

object BitlapNetworkException {

  final case class RpcException(
    override val code: Int = -1,
    override val msg: String,
    override val cause: Option[Throwable] = None
  ) extends BitlapNetworkException(msg = msg, cause = cause)

  final case class SQLExecuteException(
    override val msg: String,
    override val cause: Option[Throwable] = None
  ) extends BitlapNetworkException(msg = msg, cause = cause)

  final case class ServerIntervalException(
    override val msg: String,
    override val cause: Option[Throwable] = None
  ) extends BitlapNetworkException(msg = msg, cause = cause)

}
