/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.network

import scala.jdk.CollectionConverters.*

import org.bitlap.common.exception.BitlapException

import izumi.reflect.dottyreflection.*

/** 网络层和服务层所有通用异常
 *
 *  异常必须继承自[[org.bitlap.common.exception.BitlapException]]
 *  @author
 *    梦境迷离
 *  @since 2021/11/20
 *  @version 1.0
 */
sealed abstract class NetworkException(val code: Int = -1, val msg: String, val cause: Option[Throwable] = None)
    extends BitlapException(if msg == null then "" else msg, Map.empty[String, String].asJava, cause.orNull)
    with Product

object NetworkException:

  final case class DataFormatException(
    override val code: Int = -1,
    override val msg: String,
    override val cause: Option[Throwable] = None)
      extends NetworkException(code, msg = msg, cause = cause)

  final case class InternalException(override val msg: String, override val cause: Option[Throwable] = None)
      extends NetworkException(msg = msg, cause = cause)

  final case class LeaderNotFoundException(override val msg: String, override val cause: Option[Throwable] = None)
      extends NetworkException(msg = msg, cause = cause)

  final case class OperationMustOnLeaderException(
    override val msg: String = "This operation is not allowed on non leader nodes",
    override val cause: Option[Throwable] = None)
      extends NetworkException(msg = msg, cause = cause)

  final case class IllegalStateException(override val msg: String, override val cause: Option[Throwable] = None)
      extends NetworkException(msg = msg, cause = cause)

  final case class SQLExecutedException(override val msg: String, override val cause: Option[Throwable] = None)
      extends NetworkException(msg = msg, cause = cause)
