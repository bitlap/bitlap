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

import org.bitlap.common.exception.BitlapThrowable
import org.bitlap.server.http.Response
import org.bitlap.server.http.model.*
import org.bitlap.server.http.service.UserService
import org.bitlap.server.service.AccountAuthenticator

import io.circe.*
import io.circe.generic.auto.*
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.ztapir.*
import sttp.tapir.ztapir.query
import zio.*

object UserRoute {

  val live: ZLayer[UserService with AccountAuthenticator, Nothing, UserRoute] =
    ZLayer.fromFunction((in: UserService, accountAuthenticator: AccountAuthenticator) =>
      new UserRoute(in, accountAuthenticator)
    )
}

final class UserRoute(userService: UserService, accountAuthenticator: AccountAuthenticator)
    extends BitlapAuthRoute
    with BitlapRoute("user") {

  override val authenticator: AccountAuthenticator = accountAuthenticator

  post(_.in("login").in(jsonBody[UserLoginInput]).out(jsonBody[Response[AccountInfo]])) { in =>
    userService.login(in).response
  }

  authEndpoints {
    secureEndpoint.post.in("logout").in(jsonBody[UserLogoutInput]).out(jsonBody[Response[UserLogout]]).serverLogic {
      userInfo => input =>
        userService.logout(userInfo, input).response
    }
  }

  get(_.in("getUserByName").in(query[String]("name")).out(jsonBody[Response[AccountInfo]])) { input =>
    userService.getUserByName(input).response
  }
}
