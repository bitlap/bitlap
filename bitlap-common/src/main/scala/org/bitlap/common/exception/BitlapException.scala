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
package org.bitlap.common.exception

import java.io.IOException

class BitlapException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends Exception(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

object BitlapException {

  def unapply(arg: BitlapException): Option[(String, Map[String, String], Option[Throwable])] =
    arg match
      case null => None
      case _    => Some(arg.errorKey, arg.parameters, arg.cause)
}

class BitlapRuntimeException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends RuntimeException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

class BitlapNullPointerException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends NullPointerException(errorKey.formatErrorMessage(parameters))
    with BitlapThrowable

class BitlapIllegalArgumentException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends IllegalArgumentException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

class BitlapIllegalStateException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends IllegalStateException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

class BitlapSQLException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends RuntimeException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

class BitlapIOException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val cause: Option[Throwable] = None)
    extends IOException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

final case class DataFormatException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  cause: Option[Throwable] = None)
    extends RuntimeException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable

final case class BitlapAuthenticationException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  code: Int = 401,
  cause: Option[Throwable] = None)
    extends BitlapHttpException(errorKey, parameters, code, cause)
    with BitlapThrowable

class BitlapHttpException(
  override val errorKey: String,
  override val parameters: Map[String, String] = Map.empty,
  val code: Int,
  cause: Option[Throwable] = None)
    extends RuntimeException(errorKey.formatErrorMessage(parameters), cause.orNull)
    with BitlapThrowable
