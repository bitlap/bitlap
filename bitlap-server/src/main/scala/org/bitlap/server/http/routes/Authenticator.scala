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
import org.bitlap.server.http.model.AccountInfo
import org.bitlap.server.service.AccountAuthenticator

import zio.ZIO

trait Authenticator {

  import Authenticator._

  val authenticator: AccountAuthenticator

  // FIXME
  def authenticate(token: AuthenticationToken): ZIO[Any, BitlapAuthenticationException, AccountInfo] =
    val tk             = if (token.value.trim.contains(" ")) token.value.trim.split(" ")(1) else token.value
    val secret: String = Try(new String(Base64.getDecoder.decode(tk))).getOrElse(null)
    if (secret == null || secret.isEmpty) {
      ZIO.fail(BitlapAuthenticationException("Unauthorized"))
    } else {
      val usernamePassword = secret.split(":")
      val user             = usernamePassword(0)
      val passwd           = usernamePassword(1)
      authenticator
        .auth(user, passwd)
        .mapError(e => BitlapAuthenticationException(e.getMessage, cause = Some(e)))
    }

}

object Authenticator:
  final case class AuthenticationToken(value: String)
