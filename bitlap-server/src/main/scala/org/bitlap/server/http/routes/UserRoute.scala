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

import org.bitlap.common.exception.{ BitlapExceptions, BitlapThrowable }
import org.bitlap.server.BitlapGlobalContext
import org.bitlap.server.http.Response
import org.bitlap.server.http.model.*
import org.bitlap.server.http.service.UserService
import org.bitlap.server.service.AccountAuthenticator

import io.circe.*
import io.circe.generic.auto.*
import sttp.model.HeaderNames.Authorization
import sttp.model.headers.CookieValueWithMeta
import sttp.tapir.{ ValidationResult, Validator }
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.ztapir.*
import sttp.tapir.ztapir.query
import zio.*

object UserRoute {

  val live: ZLayer[UserService & AccountAuthenticator & BitlapGlobalContext, Nothing, UserRoute] =
    ZLayer.fromFunction(
      (userService: UserService, accountAuthenticator: AccountAuthenticator, globalContext: BitlapGlobalContext) =>
        new UserRoute(userService, accountAuthenticator, globalContext)
    )
}

final class UserRoute(
  userService: UserService,
  accountAuthenticator: AccountAuthenticator,
  globalContext: BitlapGlobalContext)
    extends BitlapAuthRoute
    with BitlapRoute("user")
    with BitlapValidator {

  override val authenticator: AccountAuthenticator = accountAuthenticator

  private val sessionConfig = globalContext.config.sessionConfig

  post(
    _.in("login")
      .in(jsonBody[UserLoginInput].validate(LoginValidator))
      .out(jsonBody[Response[AccountInfo]])
      .out(setCookie(Authorization))
      .description("login")
  ) { in =>
    userService
      .login(in)
      .mapBoth(
        ex => BitlapExceptions.unknownException(ex),
        { a =>
          Response.ok(a) ->
            CookieValueWithMeta.unsafeApply(
              value = AccountInfo.createCookieValue(in.username, in.password.getOrElse(DefaultPassword)),
              maxAge = Some(sessionConfig.timeout.toSeconds),
              httpOnly = true,
              secure = false
            )
        }
      )
  }

  authEndpoints {
    secureEndpoint.post
      .in("logout")
      .in(jsonBody[UserLogoutInput].validate(LogoutValidator))
      .out(jsonBody[Response[UserLogout]])
      .description("logout")
      .serverLogic { userInfo => input =>
        userService.logout(userInfo, input).response
      }
  }

  get(
    _.in("getUserByName")
      .in(query[String]("name").validate(NameValidator).description("getUserByName"))
      .out(jsonBody[Response[AccountInfo]])
  ) { input =>
    userService.getUserByName(input).response
  }
}
