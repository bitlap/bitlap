/* Copyright (c) 2022 bitlap.org */
package org.bitlap.network

/** All exceptions for network and server.
 *
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed trait NetworkException extends Throwable with Product {
  val code: Int
  val msg: Option[String]      = None
  val cause: Option[Throwable] = None
}

object NetworkException {

  final case class RpcException(
    code: Int,
    override val msg: Option[String] = None,
    override val cause: Option[Throwable] = None
  ) extends NetworkException

  final case class SQLExecuteException(
    code: Int,
    override val msg: Option[String] = None,
    override val cause: Option[Throwable] = None
  ) extends NetworkException

  final case class ServerIntervalException(
    code: Int,
    override val msg: Option[String] = None,
    override val cause: Option[Throwable] = None
  ) extends NetworkException

}
