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

import org.bitlap.common.exception.*
import org.bitlap.server.http.Response
import org.bitlap.server.http.model.AccountInfo

import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import sttp.tapir._
import sttp.tapir.Codec.JsonCodec
import sttp.tapir.json.circe.*
import sttp.tapir.ztapir.*
import zio.ZIO

trait BitlapAuthRoute extends BitlapRoute with Authenticator {

  import Authenticator._

  private var securityEndpoints = List[ZServerEndpoint[Any, Any]]()

  protected def authEndpoints[R](endpoints: ZServerEndpoint[R, Any]*): Unit =
    securityEndpoints = endpoints.toList.map(_.asInstanceOf[ZServerEndpoint[Any, Any]])

  def getSecurityEndpoints: List[ZServerEndpoint[Any, Any]] = securityEndpoints

  protected val secureEndpoint
    : ZPartialServerEndpoint[Nothing, AuthenticationToken, AccountInfo, Unit, BitlapThrowable, Unit, Any] = API
    .securityIn(header("Authentication").mapTo[AuthenticationToken])
    // returning the authentication error code to the user
    .zServerSecurityLogic(authenticate)
}
