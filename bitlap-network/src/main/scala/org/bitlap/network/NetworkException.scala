/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.network

import scala.jdk.CollectionConverters.*

import org.bitlap.common.exception.BitlapException

/** All common exceptions at the network and service layers.
 *
 *  Exceptions must inherit from [[org.bitlap.common.exception.BitlapException]]
 */
sealed abstract class NetworkException(
  val code: Int = -1,
  val msg: String,
  override val cause: Option[Throwable] = None)
    extends BitlapException(if msg == null then "" else msg, Map.empty[String, String], cause)
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
