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
package org.bitlap.server.http.service

import java.util.concurrent.ConcurrentHashMap

import org.bitlap.common.exception.BitlapAuthenticationException
import org.bitlap.network._
import org.bitlap.network.handles.SessionHandle
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.http.model.*
import org.bitlap.server.service.AccountAuthenticator
import org.bitlap.server.session.SessionManager

import zio.{ ZIO, ZLayer }

object UserService {

  val live: ZLayer[SessionManager & AccountAuthenticator & BitlapGlobalContext, Nothing, UserService] =
    ZLayer.fromFunction(
      (sessionManager: SessionManager, accountAuthenticator: AccountAuthenticator, context: BitlapGlobalContext) =>
        new UserService(sessionManager, accountAuthenticator, context)
    )
}

final class UserService(
  sessionManager: SessionManager,
  accountAuthenticator: AccountAuthenticator,
  context: BitlapGlobalContext) {

  private val conf = context.config.grpcConfig

  def login(input: UserLoginInput): ZIO[Any, Throwable, AccountInfo] = {
    for {
      root <- accountAuthenticator.auth(input.username, input.password.getOrElse(DefaultPassword))
      sessionId <- ZIO.attempt {
        val conn = new SyncConnection(input.username, input.password.getOrElse(DefaultPassword))
        conn.open(ServerAddress(conf.host, conf.port))
        conn.getSessionId
      }.fork
      _ <- sessionManager.frontendUserSessions.getAndUpdateZIO { coo =>
        sessionId.join.map(sessionId =>
          coo.put(root.username, sessionId)
          coo
        )
      }
    } yield {
      root
    }
  }

  def logout(accountInfo: AccountInfo, input: UserLogoutInput): ZIO[Any, Throwable, UserLogout] = {
    for {
      // check user name
      _ <- ZIO
        .when(accountInfo.username != input.username) {
          ZIO.fail(BitlapAuthenticationException("Unauthorized"))
        }
        .someOrElse(())
      _ <- sessionManager.isValidUserSession(input.username)
      _ <- sessionManager.invalidateSession(input.username)
    } yield UserLogout()
  }

  // TODO
  def getUserByName(name: String): ZIO[Any, Throwable, AccountInfo] = {
    for {
      res <-
        if (name == null) ZIO.fail(BitlapAuthenticationException("Access Denied"))
        else accountAuthenticator.getUserInfoByName(name)
    } yield { res }
  }

}
