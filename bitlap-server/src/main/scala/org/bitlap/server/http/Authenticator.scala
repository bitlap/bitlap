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
package org.bitlap.server.http

import java.util.Base64

import scala.util.Try

import org.bitlap.common.exception.*
import org.bitlap.server.http.model.*
import org.bitlap.server.service.AccountAuthenticator

import sttp.model.headers.AuthenticationScheme
import zio.ZIO

/** Verify based on the token in the request header.
 */
trait Authenticator {

  val authenticator: AccountAuthenticator

  inline def decodeToken(token: AuthenticationToken): Either[Throwable, String] = Try(
    new String(Base64.getDecoder.decode(token.value))
  ).toEither

  inline def parseFromToken(tokenType: AuthenticationType, value: String): (String, String) = {
    val usernamePassword = value.stripPrefix(tokenType.prefix).split(":")
    if (usernamePassword.length == 1) usernamePassword(0) -> DefaultPassword
    else usernamePassword(0)                              -> usernamePassword(1)
  }

  def authenticate(tokenType: AuthenticationType, token: AuthenticationToken)
    : ZIO[Any, BitlapAuthenticationException, AccountInfo] =
    val secret = decodeToken(token).map(v => parseFromToken(tokenType, v))
    secret match
      case Left(_) =>
        ZIO.fail(BitlapAuthenticationException("Unauthorized"))
      case Right(value) =>
        val (user, passwd) = value
        authenticator
          .auth(user, passwd)
          .mapError {
            case ex: BitlapAuthenticationException => ex
            case e                                 => BitlapAuthenticationException(e.getMessage, cause = Some(e))
          }
}

final case class AuthenticationToken(value: String)

enum AuthenticationType(val prefix: String):
  case Bearer extends AuthenticationType(AuthenticationScheme.Bearer.name + " ")
  case Basic  extends AuthenticationType(AuthenticationScheme.Basic.name + " ")
end AuthenticationType
