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

import org.bitlap.common.exception.BitlapAuthenticationException
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.http.model.*
import org.bitlap.server.service.AccountAuthenticator
import org.bitlap.server.session.SessionManager

import zio.{ ZIO, ZLayer }

object UserService {

  val live: ZLayer[SessionManager with AccountAuthenticator, Nothing, UserService] =
    ZLayer.fromFunction((sessionManager: SessionManager, accountAuthenticator: AccountAuthenticator) =>
      new UserService(sessionManager, accountAuthenticator)
    )
}

final class UserService(sessionManager: SessionManager, accountAuthenticator: AccountAuthenticator) {

  def login(input: UserLoginInput): ZIO[Any, Throwable, AccountInfo] = {
    for {
      root <- accountAuthenticator.auth(input.username, input.password)
      _ <- sessionManager.userSession.getAndUpdate { coo =>
        coo.put(root.username, root)
        coo
      }
    } yield {
      root
    }
  }

  def logout(accountInfo: AccountInfo, input: UserLogoutInput): ZIO[Any, Throwable, UserLogout] = {
    for {
      _ <- ZIO
        .when(accountInfo.username != input.username) {
          ZIO.fail(BitlapAuthenticationException("Unauthorized"))
        }
        .someOrElse(())
      _ <- sessionManager.userSession.getAndUpdate { coo =>
        if (coo.contains(input.username)) {
          coo.remove(input.username)
        }
        coo
      }
    } yield UserLogout()
  }

  // TODO
  def getUserByName(name: String): ZIO[Any, Throwable, AccountInfo] = {
    for {
      user <- sessionManager.userSession.get.map(_.get(name))
      res <-
        if (user == null) ZIO.fail(BitlapAuthenticationException("Access Denied"))
        else ZIO.succeed(AccountInfo.root)
    } yield { res }
  }

}
