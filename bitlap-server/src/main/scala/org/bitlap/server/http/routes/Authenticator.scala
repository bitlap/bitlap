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
package org.bitlap.server.http.routes

import java.util.Base64

import scala.util.Try

import org.bitlap.common.exception.*
import org.bitlap.server.http.model.*
import org.bitlap.server.service.AccountAuthenticator

import sttp.model.headers.AuthenticationScheme
import zio.ZIO

/** Verify based on the token in the request header
 */
trait Authenticator {

  import Authenticator._

  val authenticator: AccountAuthenticator

  // FIXME
  def authenticate(token: AuthenticationToken): ZIO[Any, BitlapAuthenticationException, AccountInfo] =
    val secret = Try(new String(Base64.getDecoder.decode(token.value))).toEither
    secret match
      case Left(_) =>
        ZIO.fail(BitlapAuthenticationException("Unauthorized"))
      case Right(value) =>
        val usernamePassword = value.stripPrefix(AuthenticationScheme.Bearer.name + " ").split(":")
        val (user, passwd) = if (usernamePassword.length == 1) {
          usernamePassword(0) -> DefaultPassword
        } else usernamePassword(0) -> usernamePassword(1)
        authenticator
          .auth(user, passwd)
          .mapError(e => BitlapAuthenticationException(e.getMessage, cause = Some(e)))

}

object Authenticator:
  final case class AuthenticationToken(value: String)
